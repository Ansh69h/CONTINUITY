import asyncio
from continuity import (
    InMemoryCausalStore,
    ContinuityRuntime,
    recoverable,
    recoverable_async,
    SideEffectClass,
    build_otlp_payload,
    InvariantChecker,
)


def test_async_recoverable_and_recovery():
    async def _run():
        store = InMemoryCausalStore()
        rt1 = ContinuityRuntime(store=store, run_id="async-run-1")
        calls = {"fetch": 0, "charge": 0}

        @recoverable_async(rt1, side_effect=SideEffectClass.PURE, step_id="fetch_quote")
        async def fetch_quote(sku: str):
            calls["fetch"] += 1
            await asyncio.sleep(0.005)
            return {"sku": sku, "price": 120}

        @recoverable_async(rt1, side_effect=SideEffectClass.IDEMPOTENT, step_id="reserve_stock")
        async def reserve_stock(sku: str):
            calls["charge"] += 1
            if calls["charge"] == 1:
                raise RuntimeError("Transient network blip")
            return {"reserved": sku}

        q = await fetch_quote("A100")
        assert q == {"sku": "A100", "price": 120}

        try:
            await reserve_stock("A100")
        except RuntimeError:
            pass

        rt2 = ContinuityRuntime(store=store, run_id="async-run-1")

        @recoverable_async(rt2, side_effect=SideEffectClass.PURE, step_id="fetch_quote")
        async def fetch_quote_v2(sku: str):
            calls["fetch"] += 1
            return {"sku": sku, "price": 999}

        @recoverable_async(rt2, side_effect=SideEffectClass.IDEMPOTENT, step_id="reserve_stock")
        async def reserve_stock_v2(sku: str):
            calls["charge"] += 1
            return {"reserved": sku}

        q2 = await fetch_quote_v2("A100")
        r2 = await reserve_stock_v2("A100")
        assert q2 == {"sku": "A100", "price": 120}
        assert r2 == {"reserved": "A100"}
        assert calls["fetch"] == 1
        assert calls["charge"] == 2
        assert InvariantChecker().check_run("async-run-1", store.get_events("async-run-1")).passed

    asyncio.run(_run())


def test_otlp_trace_export_payload():
    store = InMemoryCausalStore()
    rt = ContinuityRuntime(store=store, run_id="otlp-run")

    @recoverable(rt, side_effect=SideEffectClass.PURE)
    def step_one(x: int):
        return x + 1

    assert step_one(10) == 11
    payload = build_otlp_payload(store.get_events("otlp-run"))
    spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
    assert len(spans) == 3
    assert spans[0]["name"] == "step_one:STEP_INTENT"
