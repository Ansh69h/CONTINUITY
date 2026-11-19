from continuity import CausalEvent, EventType, InvariantChecker, SideEffectClass


def test_operator_approval_does_not_bypass_compensatable_reconciliation_invariant():
    run_id = "compensatable-approval"
    base = {
        "run_id": run_id,
        "step_id": "transfer",
        "step_name": "transfer",
        "side_effect": SideEffectClass.COMPENSATABLE,
        "idempotency_key": "stable-transfer-key",
        "timestamp": 1.0,
        "worker_id": "worker",
    }
    events = [
        CausalEvent(seq=1, event_id="intent-1", event_type=EventType.STEP_INTENT, attempt=1, **base),
        CausalEvent(seq=2, event_id="start-1", event_type=EventType.STEP_STARTED, attempt=1,
                    parent_event_ids=["intent-1"], **base),
        CausalEvent(seq=3, event_id="approval", event_type=EventType.RECOVERY_APPROVED, attempt=1,
                    parent_event_ids=["start-1"], payload={"approved": True}, **base),
        CausalEvent(seq=4, event_id="intent-2", event_type=EventType.STEP_INTENT, attempt=2,
                    parent_event_ids=["approval"], **base),
        CausalEvent(seq=5, event_id="start-2", event_type=EventType.STEP_STARTED, attempt=2,
                    parent_event_ids=["intent-2"], **base),
    ]

    report = InvariantChecker().check_run(run_id, events)

    assert any(violation.invariant_id == "INV-5" for violation in report.violations)
