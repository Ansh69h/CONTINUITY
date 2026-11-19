from concurrent.futures import ThreadPoolExecutor
import threading
import time
from continuity import ContinuityRuntime, InMemoryCausalStore, SideEffectClass
from continuity.runtime import EscalationRequiredError, LeaseContentionError


def test_completed_step_does_not_reuse_output_for_different_inputs():
    runtime = ContinuityRuntime(store=InMemoryCausalStore(), run_id="changed-inputs")
    assert runtime.step(lambda value: value, 10, step_id="calculate") == 10

    try:
        runtime.step(lambda value: value, 20, step_id="calculate")
    except EscalationRequiredError as exc:
        assert "different inputs" in exc.decision.reason
    else:
        raise AssertionError("Expected a changed-input step to halt instead of returning stale output")


def test_step_side_effect_class_cannot_change_after_history_exists():
    runtime = ContinuityRuntime(store=InMemoryCausalStore(), run_id="changed-effect-class")
    assert runtime.step(lambda value: value, 10, step_id="calculate") == 10

    try:
        runtime.step(
            lambda value: value,
            10,
            step_id="calculate",
            side_effect=SideEffectClass.IDEMPOTENT,
        )
    except EscalationRequiredError as exc:
        assert "different side-effect class" in exc.decision.reason
    else:
        raise AssertionError("Expected a changed side-effect class to halt")


def test_compensatable_failed_attempt_is_not_retried_without_protection():
    runtime = ContinuityRuntime(store=InMemoryCausalStore(), run_id="uncertain-compensation")
    external = {"count": 0}

    def mutate_then_fail():
        external["count"] += 1
        if external["count"] == 1:
            raise RuntimeError("acknowledgement lost after mutation")
        return external["count"]

    try:
        runtime.step(mutate_then_fail, step_id="charge", side_effect=SideEffectClass.COMPENSATABLE)
    except RuntimeError:
        pass

    try:
        runtime.step(mutate_then_fail, step_id="charge", side_effect=SideEffectClass.COMPENSATABLE)
    except EscalationRequiredError as exc:
        assert "without reconciler or compensator" in exc.decision.reason
    else:
        raise AssertionError("Expected uncertain compensatable effects to halt")
    assert external["count"] == 1


def test_compensatable_failed_attempt_is_compensated_before_retry():
    runtime = ContinuityRuntime(store=InMemoryCausalStore(), run_id="safe-compensation")
    external = {"count": 0, "compensations": 0, "failed_once": False}

    def mutate_then_fail_once():
        external["count"] += 1
        if not external["failed_once"]:
            external["failed_once"] = True
            raise RuntimeError("acknowledgement lost after mutation")
        return external["count"]

    def compensate(_idempotency_key):
        external["count"] -= 1
        external["compensations"] += 1

    try:
        runtime.step(
            mutate_then_fail_once,
            step_id="charge",
            side_effect=SideEffectClass.COMPENSATABLE,
            compensator=compensate,
        )
    except RuntimeError:
        pass

    result = runtime.step(
        mutate_then_fail_once,
        step_id="charge",
        side_effect=SideEffectClass.COMPENSATABLE,
        compensator=compensate,
    )
    assert result == 1
    assert external == {"count": 1, "compensations": 1, "failed_once": True}


def test_long_step_renews_lease_until_execution_finishes():
    store = InMemoryCausalStore()
    started = threading.Event()
    execution_count = {"value": 0}
    count_lock = threading.Lock()
    first_worker = ContinuityRuntime(
        store=store,
        run_id="long-running-step",
        worker_id="same-worker-id",
        lease_ttl=0.06,
    )
    second_worker = ContinuityRuntime(
        store=store,
        run_id="long-running-step",
        worker_id="same-worker-id",
        lease_ttl=0.06,
    )

    def slow_effect():
        with count_lock:
            execution_count["value"] += 1
            current = execution_count["value"]
        started.set()
        time.sleep(0.24)
        return {"execution": current}

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(first_worker.step, slow_effect, step_id="slow-step")
        assert started.wait(timeout=2)
        time.sleep(0.14)

        try:
            second_worker.step(slow_effect, step_id="slow-step")
        except LeaseContentionError:
            pass
        else:
            raise AssertionError("A second worker entered while the first worker held a renewed lease")

        assert future.result(timeout=2) == {"execution": 1}

    assert second_worker.step(slow_effect, step_id="slow-step") == {"execution": 1}
    assert execution_count["value"] == 1


def test_datetime_and_dataclass_payload_serialization_in_sqlite():
    import datetime
    import tempfile
    from dataclasses import dataclass
    from continuity import SQLiteCausalStore

    @dataclass
    class AnalysisResult:
        score: float
        tags: list[str]

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/serialization.db"
        store = SQLiteCausalStore(db_path)
        runtime = ContinuityRuntime(store=store, run_id="serial-run")

        now = datetime.datetime(2026, 11, 19, 12, 0, 0)
        dt_res = runtime.step(lambda: now, step_id="time_step")
        assert dt_res == now

        data_res = runtime.step(lambda: AnalysisResult(0.98, ["valid", "fast"]), step_id="dataclass_step")
        assert data_res.score == 0.98

        events = store.get_events("serial-run")
        assert len(events) == 6


def test_custom_object_idempotency_key_stability_across_instances():
    class TaskConfig:
        def __init__(self, mode: str, retries: int):
            self.mode = mode
            self.retries = retries

    store = InMemoryCausalStore()
    rt1 = ContinuityRuntime(store=store, run_id="config-run")
    cfg1 = TaskConfig("production", 3)
    out1 = rt1.step(lambda cfg: f"ran-{cfg.mode}-{cfg.retries}", cfg1, step_id="configure")
    assert out1 == "ran-production-3"

    # In a new runtime/process, a newly instantiated object with identical data should match
    rt2 = ContinuityRuntime(store=store, run_id="config-run")
    cfg2 = TaskConfig("production", 3)
    out2 = rt2.step(lambda cfg: f"ran-{cfg.mode}-{cfg.retries}", cfg2, step_id="configure")
    assert out2 == "ran-production-3"


def test_http_bridge_payload_change_halts_for_review():
    from continuity import HttpBridge

    class MockHttpBridge(HttpBridge):
        def request(self, step_id, method, url, payload=None, **kwargs):
            norm_method = method.upper()
            idem_key = self.runtime._compute_idempotency_key(
                self.runtime.run_id,
                step_id,
                (norm_method, url),
                {"payload_data": payload},
            ) if hasattr(self.runtime, "_compute_idempotency_key") else ""

            def _mock_do(m, u, payload_data=None):
                return {"url": u, "payload": payload_data}

            return self.runtime.step(
                _mock_do,
                norm_method,
                url,
                payload_data=payload,
                step_id=step_id,
                step_name=f"http_{method.lower()}",
            )

    store = InMemoryCausalStore()
    rt = ContinuityRuntime(store=store, run_id="http-drift-run")
    bridge = MockHttpBridge(runtime=rt)

    res1 = bridge.request("call_order", "POST", "https://api.example.com/order", payload={"amount": 100})
    assert res1 == {"url": "https://api.example.com/order", "payload": {"amount": 100}}

    try:
        bridge.request("call_order", "POST", "https://api.example.com/order", payload={"amount": 500})
    except EscalationRequiredError as exc:
        assert "different inputs" in exc.decision.reason
    else:
        raise AssertionError("Expected changing HttpBridge payload to halt instead of returning stale cached data")

