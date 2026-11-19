import asyncio

import pytest

from continuity import ContinuityRuntime, InMemoryCausalStore, InvariantChecker, SideEffectClass, SideEffectPolicy
from continuity.runtime import EscalationRequiredError


def test_explicit_operator_approval_is_durable_and_allows_one_safe_resume():
    approvals = []
    policy = SideEffectPolicy(SideEffectClass.IRREVERSIBLE, allow_operator_approval=True)
    store = InMemoryCausalStore()
    runtime = ContinuityRuntime(
        store=store,
        run_id="operator-recovery",
        operator_authorizer=lambda run_id, step_id, reason, context: approvals.append(
            (run_id, step_id, reason, context)
        ) or True,
    )
    calls = []

    def send_invoice(invoice_id):
        calls.append(invoice_id)
        if len(calls) == 1:
            raise OSError("response lost after remote effect")
        return {"invoice_id": invoice_id, "status": "sent"}

    with pytest.raises(OSError):
        runtime.step(send_invoice, "inv-42", step_id="send-invoice", policy=policy)

    approved = runtime.approve_resume(
        "send-invoice",
        "inv-42",
        policy=policy,
        reason="Operator checked the provider dashboard.",
        authorization_context={"actor": "ops-user"},
    )
    result = runtime.step(send_invoice, "inv-42", step_id="send-invoice", policy=policy)

    assert approved.event_type.value == "RECOVERY_APPROVED"
    assert result["status"] == "sent"
    assert calls == ["inv-42", "inv-42"]
    assert len(approvals) == 1
    events = store.get_events("operator-recovery")
    assert any(event.event_type.value == "RECOVERY_APPROVED" for event in events)
    assert InvariantChecker().check_run("operator-recovery", events).passed


def test_operator_approval_cannot_change_input_idempotency_key():
    policy = SideEffectPolicy(SideEffectClass.IRREVERSIBLE, allow_operator_approval=True)
    store = InMemoryCausalStore()
    runtime = ContinuityRuntime(store=store, run_id="operator-input", operator_authorizer=lambda *_: True)

    with pytest.raises(OSError):
        runtime.step(lambda value: (_ for _ in ()).throw(OSError("ambiguous")), 1,
                     step_id="send", policy=policy)

    with pytest.raises(EscalationRequiredError, match="different inputs"):
        runtime.approve_resume("send", 2, policy=policy, reason="approve")


def test_approval_requires_configured_authorizer():
    policy = SideEffectPolicy(SideEffectClass.IRREVERSIBLE, allow_operator_approval=True)
    runtime = ContinuityRuntime(store=InMemoryCausalStore(), run_id="no-authorizer")

    with pytest.raises(PermissionError, match="authorizer"):
        runtime.approve_resume("send", policy=policy, reason="approve")


def test_exhausted_attempts_cannot_record_a_useless_operator_approval():
    approvals = []
    policy = SideEffectPolicy(SideEffectClass.IRREVERSIBLE, allow_operator_approval=True)
    store = InMemoryCausalStore()
    runtime = ContinuityRuntime(
        store=store,
        run_id="exhausted-approval",
        max_attempts=1,
        operator_authorizer=lambda *_: approvals.append(True) or True,
    )

    with pytest.raises(OSError):
        runtime.step(lambda: (_ for _ in ()).throw(OSError("uncertain")), step_id="charge", policy=policy)
    event_count_before_approval = len(store.get_events("exhausted-approval"))

    with pytest.raises(ValueError, match="max_attempts"):
        runtime.approve_resume("charge", policy=policy, reason="checked provider")

    assert approvals == []
    assert len(store.get_events("exhausted-approval")) == event_count_before_approval


def test_recovery_decision_does_not_consume_operator_approved_retry():
    policy = SideEffectPolicy(SideEffectClass.IRREVERSIBLE, allow_operator_approval=True)
    store = InMemoryCausalStore()
    runtime = ContinuityRuntime(
        store=store,
        run_id="decision-attempt-budget",
        max_attempts=2,
        operator_authorizer=lambda *_: True,
    )
    calls = []

    def charge():
        calls.append("charge")
        if len(calls) == 1:
            raise OSError("remote result is uncertain")
        return "charged"

    with pytest.raises(OSError):
        runtime.step(charge, step_id="charge", policy=policy)
    with pytest.raises(EscalationRequiredError):
        runtime.step(charge, step_id="charge", policy=policy)

    runtime.approve_resume("charge", policy=policy, reason="Verified the provider state")

    assert runtime.step(charge, step_id="charge", policy=policy) == "charged"
    assert calls == ["charge", "charge"]


def test_current_reconciler_policy_is_not_overridden_by_a_stored_approval():
    permissive = SideEffectPolicy(SideEffectClass.IRREVERSIBLE, allow_operator_approval=True)
    restrictive = SideEffectPolicy(SideEffectClass.IRREVERSIBLE, require_reconciler=True)
    runtime = ContinuityRuntime(
        store=InMemoryCausalStore(),
        run_id="approval-policy-change",
        max_attempts=3,
        operator_authorizer=lambda *_: True,
    )
    calls = []
    reconciled = []

    def charge():
        calls.append("charge")
        if len(calls) == 1:
            raise OSError("uncertain charge result")
        return "charged-again"

    with pytest.raises(OSError):
        runtime.step(charge, step_id="charge", policy=permissive)
    runtime.approve_resume("charge", policy=permissive, reason="Reviewed the provider state")

    def reconcile(_key):
        reconciled.append(True)
        return True, "already-charged"

    assert runtime.step(charge, step_id="charge", policy=restrictive, reconciler=reconcile) == "already-charged"
    assert calls == ["charge"]
    assert reconciled == [True]


def test_async_current_reconciler_policy_is_not_overridden_by_a_stored_approval():
    permissive = SideEffectPolicy(SideEffectClass.IRREVERSIBLE, allow_operator_approval=True)
    restrictive = SideEffectPolicy(SideEffectClass.IRREVERSIBLE, require_reconciler=True)
    runtime = ContinuityRuntime(
        store=InMemoryCausalStore(),
        run_id="async-approval-policy-change",
        max_attempts=3,
        operator_authorizer=lambda *_: True,
    )
    calls = []
    reconciled = []

    async def charge():
        calls.append("charge")
        if len(calls) == 1:
            raise OSError("uncertain charge result")
        return "charged-again"

    async def first_attempt():
        with pytest.raises(OSError):
            await runtime.step_async(charge, step_id="charge", policy=permissive)

    asyncio.run(first_attempt())
    runtime.approve_resume("charge", policy=permissive, reason="Reviewed the provider state")

    async def reconcile(_key):
        reconciled.append(True)
        return True, "already-charged"

    result = asyncio.run(
        runtime.step_async(charge, step_id="charge", policy=restrictive, reconciler=reconcile)
    )

    assert result == "already-charged"
    assert calls == ["charge"]
    assert reconciled == [True]
