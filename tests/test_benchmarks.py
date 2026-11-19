import sys

import benchmarks.run_benchmarks as benchmark
from continuity import InMemoryCausalStore


def test_benchmark_exit_status_includes_recovery_failures(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_benchmarks.py", "--workers", "1", "--runs", "1", "--backend", "memory"],
    )
    monkeypatch.setattr(
        benchmark,
        "run",
        lambda _config: {
            "configuration": {},
            "results": {"failed_runs": 0, "recovery": {"failures": 1}},
        },
    )

    assert benchmark.main() == 1


def test_benchmark_success_returns_zero_when_workload_and_recovery_succeed(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_benchmarks.py", "--workers", "1", "--runs", "1", "--backend", "memory"],
    )
    monkeypatch.setattr(
        benchmark,
        "run",
        lambda _config: {
            "configuration": {},
            "results": {"failed_runs": 0, "recovery": {"failures": 0}},
        },
    )

    assert benchmark.main() == 0


def test_recovery_samples_use_unique_ids_for_each_benchmark_invocation():
    store = InMemoryCausalStore()

    first = benchmark._run_recovery_samples(store, 1, "benchmark-one")
    second = benchmark._run_recovery_samples(store, 1, "benchmark-two")

    assert first["samples_completed"] == second["samples_completed"] == 1
    assert store.list_runs() == ["benchmark-one-recovery-0", "benchmark-two-recovery-0"]
