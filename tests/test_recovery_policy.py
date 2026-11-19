import asyncio

import pytest

from continuity import ContinuityRuntime, InMemoryCausalStore, SideEffectClass, SideEffectPolicy
from continuity.runtime import recoverable, recoverable_async


@pytest.mark.parametrize("classification", list(SideEffectClass))
def test_policy_maps_to_existing_side_effect_classes(classification):
    policy = SideEffectPolicy(classification)

    assert policy.side_effect == classification


def test_policy_requires_callbacks_it_declares_mandatory():
    policy = SideEffectPolicy(SideEffectClass.IRREVERSIBLE, require_reconciler=True)

    assert policy.require_reconciler is True
    assert policy.allow_operator_approval is False


def test_policy_rejects_blind_retry_of_uncertain_effects():
    with pytest.raises(ValueError, match="cannot be retried without reconciliation"):
        SideEffectPolicy(
            SideEffectClass.IRREVERSIBLE,
            retry_uncertain_without_reconciliation=True,
        )


def test_recoverable_preserves_legacy_positional_step_id_argument():
    store = InMemoryCausalStore()
    runtime = ContinuityRuntime(store=store, run_id="legacy-decorator")

    @recoverable(runtime, SideEffectClass.PURE, "legacy-step-id")
    def fetch_value():
        return "value"

    assert fetch_value() == "value"
    assert store.get_events("legacy-decorator")[0].step_id == "legacy-step-id"


def test_recoverable_async_preserves_legacy_positional_step_id_argument():
    store = InMemoryCausalStore()
    runtime = ContinuityRuntime(store=store, run_id="legacy-async-decorator")

    @recoverable_async(runtime, SideEffectClass.PURE, "legacy-async-step-id")
    async def fetch_value():
        return "value"

    assert asyncio.run(fetch_value()) == "value"
    assert store.get_events("legacy-async-decorator")[0].step_id == "legacy-async-step-id"
