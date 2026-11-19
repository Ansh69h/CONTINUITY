from continuity.validation.harness import (
    StatefulModelChecker,
    run_reliability_campaign,
)
from continuity.validation.invariants import InvariantChecker, SEVEN_INVARIANTS
from continuity.models import CausalEvent, EventType, SideEffectClass


def test_release_gate_10000_randomized_stateful_trials():
    """Release Gate: 10,000 randomized stateful trials with zero invariant violations."""
    report = run_reliability_campaign(
        num_trials=10_000,
        base_seed=20260800,
        max_attempts=3,
        sqlite_sample_stride=500,
    )
    assert report.total_trials == 10_000
    assert report.failed_trials == 0, (
        f"Failed {report.failed_trials} trials; first failure repro: "
        f"{report.failures[0].to_repro_bundle()}"
    )
    assert report.passed_trials == 10_000
    assert report.total_restarts > 1_000
    assert report.total_escalations > 500


def test_deterministic_seed_reproducibility():
    """Any seed produces byte-for-byte identical execution traces and outcomes."""
    checker = StatefulModelChecker(max_attempts=3, use_sqlite=False)
    r1 = checker.run_trial(trial_index=77, seed=99887766)
    r2 = checker.run_trial(trial_index=77, seed=99887766)
    assert r1.to_repro_bundle() == r2.to_repro_bundle()


def test_all_seven_invariants_catch_corrupted_logs():
    """Verify each of the 7 documented safety invariants actively detects its target corruption."""
    assert len(SEVEN_INVARIANTS) == 7
    checker = InvariantChecker(max_attempts=2)

    ev_gap = CausalEvent(
        run_id="r1", seq=2, event_id="e1", step_id="s1", step_name="s1",
        event_type=EventType.STEP_INTENT, side_effect=SideEffectClass.PURE, idempotency_key="k1"
    )
    assert any(v.invariant_id == "INV-1" for v in checker.check_run("r1", [ev_gap]).violations)

    ev_no_intent = CausalEvent(
        run_id="r1", seq=1, event_id="e1", step_id="s1", step_name="s1",
        event_type=EventType.STEP_STARTED, side_effect=SideEffectClass.PURE, idempotency_key="k1"
    )
    assert any(v.invariant_id == "INV-2" for v in checker.check_run("r1", [ev_no_intent]).violations)

    ev_i = CausalEvent("r1", 1, "e1", "s1", "s1", EventType.STEP_INTENT, SideEffectClass.PURE, "k1")
    ev_s = CausalEvent("r1", 2, "e2", "s1", "s1", EventType.STEP_STARTED, SideEffectClass.PURE, "k1", ["e1"])
    ev_c1 = CausalEvent("r1", 3, "e3", "s1", "s1", EventType.STEP_COMMITTED, SideEffectClass.PURE, "k1", ["e2"], {"output": 1})
    ev_c2 = CausalEvent("r1", 4, "e4", "s1", "s1", EventType.STEP_COMMITTED, SideEffectClass.PURE, "k1", ["e3"], {"output": 2})
    assert any(v.invariant_id == "INV-3" for v in checker.check_run("r1", [ev_i, ev_s, ev_c1, ev_c2]).violations)

    ev_ir1 = CausalEvent("r1", 1, "e1", "s1", "s1", EventType.STEP_INTENT, SideEffectClass.IRREVERSIBLE, "k1")
    ev_ir2 = CausalEvent("r1", 2, "e2", "s1", "s1", EventType.STEP_STARTED, SideEffectClass.IRREVERSIBLE, "k1", ["e1"])
    ev_ir3 = CausalEvent("r1", 3, "e3", "s1", "s1", EventType.STEP_INTENT, SideEffectClass.IRREVERSIBLE, "k1", ["e2"])
    ev_ir4 = CausalEvent("r1", 4, "e4", "s1", "s1", EventType.STEP_STARTED, SideEffectClass.IRREVERSIBLE, "k1", ["e3"])
    assert any(v.invariant_id == "INV-4" for v in checker.check_run("r1", [ev_ir1, ev_ir2, ev_ir3, ev_ir4]).violations)

    ev_cp1 = CausalEvent("r1", 1, "e1", "s1", "s1", EventType.STEP_INTENT, SideEffectClass.COMPENSATABLE, "k1")
    ev_cp2 = CausalEvent("r1", 2, "e2", "s1", "s1", EventType.STEP_STARTED, SideEffectClass.COMPENSATABLE, "k1", ["e1"])
    ev_cp3 = CausalEvent("r1", 3, "e3", "s1", "s1", EventType.STEP_INTENT, SideEffectClass.COMPENSATABLE, "k1", ["e2"])
    assert any(v.invariant_id == "INV-5" for v in checker.check_run("r1", [ev_cp1, ev_cp2, ev_cp3]).violations)

    ev_id1 = CausalEvent("r1", 1, "e1", "s1", "s1", EventType.STEP_INTENT, SideEffectClass.PURE, "key-A")
    ev_id2 = CausalEvent("r1", 2, "e2", "s1", "s1", EventType.STEP_STARTED, SideEffectClass.PURE, "key-B", ["e1"])
    assert any(v.invariant_id == "INV-6" for v in checker.check_run("r1", [ev_id1, ev_id2]).violations)

    ev_at = CausalEvent("r1", 1, "e1", "s1", "s1", EventType.STEP_STARTED, SideEffectClass.PURE, "k1", attempt=3)
    assert any(v.invariant_id == "INV-7" for v in checker.check_run("r1", [ev_at]).violations)
