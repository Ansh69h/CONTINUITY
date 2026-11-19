from concurrent.futures import ThreadPoolExecutor
import os
import tempfile
import threading
import time
from continuity import (
    SQLiteCausalStore,
    ContinuityRuntime,
    SideEffectClass,
    InvariantChecker,
)
from continuity.runtime import LeaseContentionError


def test_concurrent_workers_contending_on_same_step():
    """12 concurrent workers race to execute the exact same step_id on a shared SQLite WAL store."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "concurrent.db")
        store = SQLiteCausalStore(db_path)
        exec_counter = 0
        counter_lock = threading.Lock()

        def worker_task(worker_idx: int):
            rt = ContinuityRuntime(
                store=store,
                run_id="shared-concurrent-run",
                worker_id=f"worker-{worker_idx}",
            )

            def expensive_side_effect(x: int):
                nonlocal exec_counter
                time.sleep(0.02)
                with counter_lock:
                    exec_counter += 1
                    cur = exec_counter
                return {"x": x, "execution_instance": cur}

            for _ in range(25):
                try:
                    return rt.step(
                        expensive_side_effect,
                        42,
                        step_id="critical_shared_step",
                        side_effect=SideEffectClass.IRREVERSIBLE,
                    )
                except LeaseContentionError:
                    time.sleep(0.01)
            raise TimeoutError("Worker never observed committed result")

        with ThreadPoolExecutor(max_workers=12) as pool:
            futures = [pool.submit(worker_task, i) for i in range(12)]
            results = [f.result(timeout=15) for f in futures]

        assert exec_counter == 1
        assert all(r == {"x": 42, "execution_instance": 1} for r in results)

        events = store.get_events("shared-concurrent-run")
        report = InvariantChecker().check_run(
            "shared-concurrent-run",
            events,
            external_effect_counts={"critical_shared_step": exec_counter},
        )
        assert report.passed, f"Violations: {report.violations}"
