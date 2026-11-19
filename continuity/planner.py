from __future__ import annotations
from typing import List
from .models import (
    CausalEvent,
    EventType,
    SideEffectClass,
    RecoveryAction,
    RecoveryDecision,
)


class RecoveryPlanner:
    """Deterministic causal recovery planner enforcing side-effect safety rules."""

    def analyze_step(
        self,
        events: List[CausalEvent],
        step_id: str,
        step_name: str,
        side_effect: SideEffectClass,
        has_reconciler: bool = False,
        has_compensator: bool = False,
        max_attempts: int = 3,
    ) -> RecoveryDecision:
        step_events = [e for e in events if e.step_id == step_id]
        if not step_events:
            return RecoveryDecision(
                step_id=step_id,
                step_name=step_name,
                action=RecoveryAction.REEXECUTE,
                reason="Fresh step with no prior causal history.",
                attempt=1,
            )

        committed = [e for e in step_events if e.event_type == EventType.STEP_COMMITTED]
        if committed:
            latest_commit = committed[-1]
            return RecoveryDecision(
                step_id=step_id,
                step_name=step_name,
                action=RecoveryAction.REUSE_CACHED,
                reason="Step already committed; reusing deterministic output without re-execution.",
                cached_output=latest_commit.payload.get("output"),
                attempt=latest_commit.attempt,
            )

        started_events = [e for e in step_events if e.event_type == EventType.STEP_STARTED]
        failed_events = [e for e in step_events if e.event_type == EventType.STEP_FAILED]
        comp_committed = [e for e in step_events if e.event_type == EventType.COMPENSATION_COMMITTED]
        attempts_so_far = max([e.attempt for e in step_events], default=1)

        if attempts_so_far >= max_attempts:
            return RecoveryDecision(
                step_id=step_id,
                step_name=step_name,
                action=RecoveryAction.HALT_ESCALATE,
                reason=f"Exceeded max_attempts ({max_attempts}); escalating safely.",
                attempt=attempts_so_far + 1,
            )

        in_flight = len(started_events) > len(failed_events)

        if side_effect == SideEffectClass.PURE:
            return RecoveryDecision(
                step_id=step_id,
                step_name=step_name,
                action=RecoveryAction.REEXECUTE,
                reason="PURE step is side-effect free; safe to re-execute.",
                attempt=attempts_so_far + 1,
            )

        if side_effect == SideEffectClass.IDEMPOTENT:
            if in_flight and has_reconciler:
                return RecoveryDecision(
                    step_id=step_id,
                    step_name=step_name,
                    action=RecoveryAction.RECONCILE_EXTERNAL,
                    reason="IDEMPOTENT step crashed in-flight; probing external state via reconciler first.",
                    attempt=attempts_so_far + 1,
                )
            return RecoveryDecision(
                step_id=step_id,
                step_name=step_name,
                action=RecoveryAction.REEXECUTE,
                reason="IDEMPOTENT step safe to retry with identical idempotency_key.",
                attempt=attempts_so_far + 1,
            )

        if side_effect == SideEffectClass.COMPENSATABLE:
            # A STEP_FAILED event only says the callable raised. It does not
            # prove that its external effect did not happen before the error.
            # Treat every started but uncompensated attempt as uncertain.
            unresolved_starts = len(started_events) - len(comp_committed)
            if unresolved_starts > 0:
                if has_reconciler:
                    return RecoveryDecision(
                        step_id=step_id,
                        step_name=step_name,
                        action=RecoveryAction.RECONCILE_EXTERNAL,
                        reason="COMPENSATABLE step crashed in-flight; reconciling external effect before retry.",
                        attempt=attempts_so_far + 1,
                    )
                if has_compensator:
                    return RecoveryDecision(
                        step_id=step_id,
                        step_name=step_name,
                        action=RecoveryAction.COMPENSATE_AND_RETRY,
                        reason="COMPENSATABLE step crashed in-flight; running compensating action before retry.",
                        attempt=attempts_so_far + 1,
                    )
                return RecoveryDecision(
                    step_id=step_id,
                    step_name=step_name,
                    action=RecoveryAction.HALT_ESCALATE,
                    reason="COMPENSATABLE step crashed in-flight without reconciler or compensator.",
                    attempt=attempts_so_far + 1,
                )
            return RecoveryDecision(
                step_id=step_id,
                step_name=step_name,
                action=RecoveryAction.REEXECUTE,
                reason="COMPENSATABLE step failed prior to commit; safe to re-execute.",
                attempt=attempts_so_far + 1,
            )

        # SideEffectClass.IRREVERSIBLE
        if not in_flight and len(started_events) == 0:
            return RecoveryDecision(
                step_id=step_id,
                step_name=step_name,
                action=RecoveryAction.REEXECUTE,
                reason="IRREVERSIBLE step never started execution; safe to execute once.",
                attempt=1,
            )

        if has_reconciler:
            return RecoveryDecision(
                step_id=step_id,
                step_name=step_name,
                action=RecoveryAction.RECONCILE_EXTERNAL,
                reason="IRREVERSIBLE step attempted previously; reconciling external state before any retry.",
                attempt=attempts_so_far + 1,
            )

        return RecoveryDecision(
            step_id=step_id,
            step_name=step_name,
            action=RecoveryAction.HALT_ESCALATE,
            reason="IRREVERSIBLE step cannot be blindly retried without external reconciliation.",
            attempt=attempts_so_far + 1,
        )


class AsyncRecoveryPlanner(RecoveryPlanner):
    """Async-compatible recovery planner supporting coroutine reconcilers."""

    async def analyze_step_async(
        self,
        events: List[CausalEvent],
        step_id: str,
        step_name: str,
        side_effect: SideEffectClass,
        has_reconciler: bool = False,
        has_compensator: bool = False,
        max_attempts: int = 3,
    ) -> RecoveryDecision:
        return self.analyze_step(
            events=events,
            step_id=step_id,
            step_name=step_name,
            side_effect=side_effect,
            has_reconciler=has_reconciler,
            has_compensator=has_compensator,
            max_attempts=max_attempts,
        )
