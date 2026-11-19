from __future__ import annotations
import os
import random
import tempfile
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Tuple
from ..models import SideEffectClass, InvariantViolation
from ..storage.base import InMemoryCausalStore, SQLiteCausalStore
from ..runtime import ContinuityRuntime, EscalationRequiredError
from .invariants import InvariantChecker


class SimulatedCrashError(RuntimeError):
    """Simulates sudden worker termination mid-step."""


class SimulatedTransientError(RuntimeError):
    """Simulates transient network/service timeout before side-effect commit."""


@dataclass
class FaultInjector:
    rng: random.Random
    crash_after_start_prob: float = 0.25
    crash_before_commit_prob: float = 0.20
    transient_fail_prob: float = 0.20
    trace: List[str] = field(default_factory=list)

    def should_crash_after_start(self, step_id: str, attempt: int) -> bool:
        hit = self.rng.random() < self.crash_after_start_prob
        if hit:
            self.trace.append(f"FAULT:crash_after_start:{step_id}:attempt={attempt}")
        return hit

    def should_fail_transient(self, step_id: str, attempt: int) -> bool:
        hit = self.rng.random() < self.transient_fail_prob
        if hit:
            self.trace.append(f"FAULT:transient_error:{step_id}:attempt={attempt}")
        return hit

    def should_crash_before_commit(self, step_id: str, attempt: int) -> bool:
        hit = self.rng.random() < self.crash_before_commit_prob
        if hit:
            self.trace.append(f"FAULT:crash_before_commit:{step_id}:attempt={attempt}")
        return hit


@dataclass
class TrialResult:
    trial_index: int
    seed: int
    run_id: str
    passed: bool
    completed_steps: int
    escalated_steps: int
    restarts: int
    trace: List[str]
    violations: List[InvariantViolation] = field(default_factory=list)

    def to_repro_bundle(self) -> Dict[str, Any]:
        return {
            "trial_index": self.trial_index,
            "seed": self.seed,
            "run_id": self.run_id,
            "passed": self.passed,
            "completed_steps": self.completed_steps,
            "escalated_steps": self.escalated_steps,
            "restarts": self.restarts,
            "trace": self.trace,
            "violations": [asdict(v) for v in self.violations],
        }


@dataclass
class ValidationCampaignReport:
    total_trials: int
    passed_trials: int
    failed_trials: int
    total_restarts: int
    total_escalations: int
    failures: List[TrialResult] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return self.failed_trials == 0 and self.passed_trials == self.total_trials


class StatefulModelChecker:
    """Randomized stateful model checker with deterministic seed replay and full 7-invariant verification."""

    def __init__(self, max_attempts: int = 3, use_sqlite: bool = False) -> None:
        self.max_attempts = max_attempts
        self.use_sqlite = use_sqlite
        self.checker = InvariantChecker(max_attempts=max_attempts)

    def run_trial(self, trial_index: int, seed: int) -> TrialResult:
        rng = random.Random(seed)
        faults = FaultInjector(rng=rng)
        run_id = f"trial-{trial_index}-seed-{seed}"

        tmp_file = None
        if self.use_sqlite:
            fd, tmp_file = tempfile.mkstemp(prefix=f"cont_{seed}_", suffix=".db")
            os.close(fd)
            store = SQLiteCausalStore(tmp_file)
        else:
            store = InMemoryCausalStore()

        try:
            num_steps = rng.randint(3, 6)
            step_specs: List[Tuple[str, SideEffectClass, bool, bool]] = []
            classes = [
                SideEffectClass.PURE,
                SideEffectClass.IDEMPOTENT,
                SideEffectClass.COMPENSATABLE,
                SideEffectClass.IRREVERSIBLE,
            ]
            for idx in range(num_steps):
                se = rng.choice(classes)
                has_rec = rng.choice([True, False]) if se != SideEffectClass.PURE else False
                has_comp = rng.choice([True, False]) if se == SideEffectClass.COMPENSATABLE else False
                step_specs.append((f"step_{idx}_{se.value.lower()}", se, has_rec, has_comp))

            external_ledger: Dict[str, Any] = {}
            external_exec_counts: Dict[str, int] = {}
            compensated_keys: set[str] = set()
            completed = 0
            escalated = 0
            restarts = 0

            for s_id, se, has_rec, has_comp in step_specs:
                step_arg = rng.randint(1, 50)
                step_done = False
                while not step_done:
                    rt = ContinuityRuntime(
                        store=store,
                        run_id=run_id,
                        worker_id=f"worker-r{restarts}",
                        max_attempts=self.max_attempts,
                    )

                    def make_fn(sid: str):
                        def _exec(val: int) -> Dict[str, Any]:
                            attempt_num = external_exec_counts.get(sid, 0) + 1
                            if faults.should_fail_transient(sid, attempt_num):
                                raise SimulatedTransientError(f"Transient failure in {sid}")
                            external_exec_counts[sid] = attempt_num
                            out = {"step": sid, "value": val * 10, "exec_num": attempt_num}
                            external_ledger[sid] = out
                            faults.trace.append(f"EXEC:{sid}:count={attempt_num}")
                            return out
                        return _exec

                    def make_reconciler(sid: str):
                        def _rec(idem_key: str) -> Tuple[bool, Any]:
                            faults.trace.append(f"RECONCILE:{sid}")
                            if sid in external_ledger and sid not in compensated_keys:
                                return (True, external_ledger[sid])
                            return (False, None)
                        return _rec

                    def make_compensator(sid: str):
                        def _comp(idem_key: str) -> Any:
                            faults.trace.append(f"COMPENSATE:{sid}")
                            compensated_keys.add(sid)
                            external_ledger.pop(sid, None)
                            return {"compensated": sid}
                        return _comp

                    def after_start(sid: str = s_id):
                        if faults.should_crash_after_start(sid, external_exec_counts.get(sid, 0) + 1):
                            raise SimulatedCrashError(f"Crash after start on {sid}")

                    def before_commit(sid: str = s_id):
                        if faults.should_crash_before_commit(sid, external_exec_counts.get(sid, 0)):
                            raise SimulatedCrashError(f"Crash before commit on {sid}")

                    try:
                        res = rt.step(
                            make_fn(s_id),
                            step_arg,
                            step_id=s_id,
                            step_name=s_id,
                            side_effect=se,
                            reconciler=make_reconciler(s_id) if has_rec else None,
                            compensator=make_compensator(s_id) if has_comp else None,
                            after_start_hook=after_start,
                            before_commit_hook=before_commit,
                        )
                        faults.trace.append(f"COMMITTED:{s_id}:{res}")
                        completed += 1
                        step_done = True
                    except SimulatedCrashError:
                        restarts += 1
                        faults.trace.append(f"RESTART:{restarts}")
                    except SimulatedTransientError:
                        pass
                    except EscalationRequiredError as esc:
                        faults.trace.append(f"ESCALATED:{s_id}:{esc.decision.reason}")
                        escalated += 1
                        step_done = True

            events = store.get_events(run_id)
            report = self.checker.check_run(
                run_id=run_id,
                events=events,
                external_effect_counts=external_exec_counts,
            )
            return TrialResult(
                trial_index=trial_index,
                seed=seed,
                run_id=run_id,
                passed=report.passed,
                completed_steps=completed,
                escalated_steps=escalated,
                restarts=restarts,
                trace=faults.trace,
                violations=report.violations,
            )
        finally:
            if tmp_file and os.path.exists(tmp_file):
                try:
                    os.remove(tmp_file)
                except OSError:
                    pass
                for suffix in ("-wal", "-shm"):
                    if os.path.exists(tmp_file + suffix):
                        try:
                            os.remove(tmp_file + suffix)
                        except OSError:
                            pass


def run_reliability_campaign(
    num_trials: int = 10_000,
    base_seed: int = 20260800,
    max_attempts: int = 3,
    sqlite_sample_stride: int = 250,
) -> ValidationCampaignReport:
    passed = 0
    failed = 0
    total_restarts = 0
    total_escalations = 0
    failures: List[TrialResult] = []

    mem_checker = StatefulModelChecker(max_attempts=max_attempts, use_sqlite=False)
    sql_checker = StatefulModelChecker(max_attempts=max_attempts, use_sqlite=True)

    for i in range(num_trials):
        seed = base_seed + i
        checker = sql_checker if (sqlite_sample_stride > 0 and i % sqlite_sample_stride == 0) else mem_checker
        res = checker.run_trial(trial_index=i, seed=seed)
        total_restarts += res.restarts
        total_escalations += res.escalated_steps
        if res.passed:
            passed += 1
        else:
            failed += 1
            failures.append(res)

    return ValidationCampaignReport(
        total_trials=num_trials,
        passed_trials=passed,
        failed_trials=failed,
        total_restarts=total_restarts,
        total_escalations=total_escalations,
        failures=failures,
    )
