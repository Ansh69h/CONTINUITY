from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from ..models import CausalEvent, EventType, SideEffectClass, InvariantViolation


SEVEN_INVARIANTS: Dict[str, str] = {
    "INV-1": "Causal Log Monotonicity & Parent Integrity — Strictly contiguous 1..N sequence numbers with valid causal parent references.",
    "INV-2": "Intent-Before-Effect Ordering — Every STEP_STARTED or STEP_COMMITTED must be preceded by a matching STEP_INTENT.",
    "INV-3": "Committed Output Immutability & Single-Commit — Once STEP_COMMITTED is recorded for a step_id, output cannot mutate and no duplicate commits or re-executions may occur.",
    "INV-4": "Irreversible Side-Effect Safety — An IRREVERSIBLE retry requires external reconciliation proof or a durable authorized operator approval.",
    "INV-5": "Compensatable Crash Recovery Discipline — A COMPENSATABLE step with an uncommitted STEP_STARTED cannot retry until compensated or reconciled.",
    "INV-6": "Idempotency Key Stability — All events belonging to the same step_id within a run carry an identical idempotency_key.",
    "INV-7": "Bounded Recovery Termination — No step exceeds configured max_attempts without halting/escalating.",
}


@dataclass
class InvariantReport:
    run_id: str
    passed: bool
    checked_invariants: List[str]
    violations: List[InvariantViolation] = field(default_factory=list)
    event_count: int = 0


class InvariantChecker:
    """Automated verifier for all seven documented CONTINUITY safety invariants."""

    def __init__(self, max_attempts: int = 3) -> None:
        self.max_attempts = max_attempts

    def check_run(
        self,
        run_id: str,
        events: List[CausalEvent],
        external_effect_counts: Optional[Dict[str, int]] = None,
    ) -> InvariantReport:
        violations: List[InvariantViolation] = []
        seen_event_ids = set()

        step_intents: Dict[str, int] = {}
        step_started: Dict[str, int] = {}
        step_failed: Dict[str, int] = {}
        step_committed: Dict[str, List[CausalEvent]] = {}
        step_comp_committed: Dict[str, int] = {}
        step_idem_keys: Dict[str, str] = {}
        step_side_effects: Dict[str, SideEffectClass] = {}
        reconciled_retry_authorized: Dict[str, bool] = {}
        operator_retry_authorized: Dict[str, bool] = {}

        for idx, ev in enumerate(events):
            expected_seq = idx + 1

            # INV-1: Monotonicity & Parent Integrity
            if ev.seq != expected_seq:
                violations.append(
                    InvariantViolation(
                        invariant_id="INV-1",
                        invariant_name=SEVEN_INVARIANTS["INV-1"],
                        run_id=run_id,
                        step_id=ev.step_id,
                        seq=ev.seq,
                        description=f"Non-monotonic sequence number: expected {expected_seq}, found {ev.seq}.",
                    )
                )
            for pid in ev.parent_event_ids:
                if pid not in seen_event_ids:
                    violations.append(
                        InvariantViolation(
                            invariant_id="INV-1",
                            invariant_name=SEVEN_INVARIANTS["INV-1"],
                            run_id=run_id,
                            step_id=ev.step_id,
                            seq=ev.seq,
                            description=f"Causal parent {pid} not found prior to event {ev.event_id}.",
                        )
                    )
            seen_event_ids.add(ev.event_id)

            # INV-6: Idempotency Key Stability
            if ev.step_id in step_idem_keys:
                if step_idem_keys[ev.step_id] != ev.idempotency_key:
                    violations.append(
                        InvariantViolation(
                            invariant_id="INV-6",
                            invariant_name=SEVEN_INVARIANTS["INV-6"],
                            run_id=run_id,
                            step_id=ev.step_id,
                            seq=ev.seq,
                            description=(
                                f"Idempotency key drifted for step {ev.step_id}: "
                                f"{step_idem_keys[ev.step_id]} != {ev.idempotency_key}"
                            ),
                        )
                    )
            else:
                step_idem_keys[ev.step_id] = ev.idempotency_key

            step_side_effects[ev.step_id] = ev.side_effect

            # INV-7: Bounded Recovery Termination
            if ev.attempt > self.max_attempts and ev.event_type == EventType.STEP_STARTED:
                violations.append(
                    InvariantViolation(
                        invariant_id="INV-7",
                        invariant_name=SEVEN_INVARIANTS["INV-7"],
                        run_id=run_id,
                        step_id=ev.step_id,
                        seq=ev.seq,
                        description=f"Step {ev.step_id} started attempt {ev.attempt} exceeding max_attempts={self.max_attempts}.",
                    )
                )

            if ev.event_type == EventType.RECOVERY_DECISION:
                if ev.payload.get("action") == "RECONCILE_EXTERNAL":
                    reconciled_retry_authorized[ev.step_id] = True

            elif ev.event_type == EventType.RECOVERY_APPROVED:
                if ev.payload.get("approved") is True:
                    operator_retry_authorized[ev.step_id] = True

            elif ev.event_type == EventType.STEP_INTENT:
                if ev.step_id in step_committed:
                    violations.append(
                        InvariantViolation(
                            invariant_id="INV-3",
                            invariant_name=SEVEN_INVARIANTS["INV-3"],
                            run_id=run_id,
                            step_id=ev.step_id,
                            seq=ev.seq,
                            description=f"New STEP_INTENT emitted after step {ev.step_id} was already committed.",
                        )
                    )
                if ev.side_effect == SideEffectClass.COMPENSATABLE:
                    uncommitted_starts = step_started.get(ev.step_id, 0) - step_failed.get(ev.step_id, 0)
                    if uncommitted_starts > step_comp_committed.get(ev.step_id, 0) and not reconciled_retry_authorized.get(ev.step_id, False):
                        violations.append(
                            InvariantViolation(
                                invariant_id="INV-5",
                                invariant_name=SEVEN_INVARIANTS["INV-5"],
                                run_id=run_id,
                                step_id=ev.step_id,
                                seq=ev.seq,
                                description=f"COMPENSATABLE step {ev.step_id} retried without completing compensation or reconciliation.",
                            )
                        )
                    if uncommitted_starts > step_comp_committed.get(ev.step_id, 0):
                        reconciled_retry_authorized[ev.step_id] = False
                step_intents[ev.step_id] = step_intents.get(ev.step_id, 0) + 1

            elif ev.event_type == EventType.STEP_STARTED:
                if step_intents.get(ev.step_id, 0) <= step_started.get(ev.step_id, 0):
                    violations.append(
                        InvariantViolation(
                            invariant_id="INV-2",
                            invariant_name=SEVEN_INVARIANTS["INV-2"],
                            run_id=run_id,
                            step_id=ev.step_id,
                            seq=ev.seq,
                            description=f"STEP_STARTED recorded without preceding STEP_INTENT for {ev.step_id}.",
                        )
                    )
                if ev.step_id in step_committed:
                    violations.append(
                        InvariantViolation(
                            invariant_id="INV-3",
                            invariant_name=SEVEN_INVARIANTS["INV-3"],
                            run_id=run_id,
                            step_id=ev.step_id,
                            seq=ev.seq,
                            description=f"STEP_STARTED recorded after {ev.step_id} already committed.",
                        )
                    )
                step_started[ev.step_id] = step_started.get(ev.step_id, 0) + 1

                if ev.side_effect == SideEffectClass.IRREVERSIBLE and step_started[ev.step_id] > 1:
                    if not (
                        reconciled_retry_authorized.get(ev.step_id, False)
                        or operator_retry_authorized.get(ev.step_id, False)
                    ):
                        violations.append(
                            InvariantViolation(
                                invariant_id="INV-4",
                                invariant_name=SEVEN_INVARIANTS["INV-4"],
                                run_id=run_id,
                                step_id=ev.step_id,
                                seq=ev.seq,
                                description=f"IRREVERSIBLE step {ev.step_id} started {step_started[ev.step_id]} times without reconciliation.",
                            )
                        )
                if ev.side_effect == SideEffectClass.IRREVERSIBLE:
                    reconciled_retry_authorized[ev.step_id] = False
                    operator_retry_authorized[ev.step_id] = False

            elif ev.event_type == EventType.STEP_FAILED:
                step_failed[ev.step_id] = step_failed.get(ev.step_id, 0) + 1

            elif ev.event_type == EventType.COMPENSATION_COMMITTED:
                step_comp_committed[ev.step_id] = step_comp_committed.get(ev.step_id, 0) + 1

            elif ev.event_type == EventType.STEP_COMMITTED:
                is_reconciled = bool(ev.payload.get("reconciled", False))
                if not is_reconciled and step_intents.get(ev.step_id, 0) == 0:
                    violations.append(
                        InvariantViolation(
                            invariant_id="INV-2",
                            invariant_name=SEVEN_INVARIANTS["INV-2"],
                            run_id=run_id,
                            step_id=ev.step_id,
                            seq=ev.seq,
                            description=f"Unreconciled STEP_COMMITTED without STEP_INTENT for {ev.step_id}.",
                        )
                    )
                prior_commits = step_committed.setdefault(ev.step_id, [])
                if prior_commits:
                    first_out = prior_commits[0].payload.get("output")
                    curr_out = ev.payload.get("output")
                    violations.append(
                        InvariantViolation(
                            invariant_id="INV-3",
                            invariant_name=SEVEN_INVARIANTS["INV-3"],
                            run_id=run_id,
                            step_id=ev.step_id,
                            seq=ev.seq,
                            description=(
                                f"Duplicate STEP_COMMITTED for {ev.step_id} "
                                f"(first={first_out!r}, duplicate={curr_out!r})."
                            ),
                        )
                    )
                prior_commits.append(ev)

        if external_effect_counts:
            for s_id, count in external_effect_counts.items():
                se = step_side_effects.get(s_id)
                if se == SideEffectClass.IRREVERSIBLE and count > 1:
                    violations.append(
                        InvariantViolation(
                            invariant_id="INV-4",
                            invariant_name=SEVEN_INVARIANTS["INV-4"],
                            run_id=run_id,
                            step_id=s_id,
                            seq=None,
                            description=f"External side-effect counter recorded {count} executions for IRREVERSIBLE step {s_id}.",
                        )
                    )

        return InvariantReport(
            run_id=run_id,
            passed=len(violations) == 0,
            checked_invariants=list(SEVEN_INVARIANTS.keys()),
            violations=violations,
            event_count=len(events),
        )
