"""Repeatable local throughput/contention benchmark for CONTINUITY.

Examples:
    python benchmarks/run_benchmarks.py --workers 8 --runs 500 --backend sqlite
    python benchmarks/run_benchmarks.py --scenario 100w-10k --backend sqlite

Scenario entries are configurations only. This script writes measured results to JSON.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import itertools
import json
import os
import statistics
import tempfile
import threading
import time
import tracemalloc
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from continuity import ContinuityRuntime, InMemoryCausalStore, SQLiteCausalStore


SCENARIOS_PATH = Path(__file__).with_name("scenarios.json")
_thread_state = threading.local()


@dataclass(frozen=True)
class Config:
    workers: int
    runs: int
    backend: str
    database: Path | None
    recovery_samples: int
    contention_groups: int


def _scenario_map() -> dict[str, dict[str, int]]:
    data = json.loads(SCENARIOS_PATH.read_text(encoding="utf-8"))
    return {entry["id"]: entry for entry in data["scenarios"]}


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((percentile / 100) * (len(ordered) - 1))))
    return ordered[index]


def _create_store(backend: str, database: Path | None) -> Any:
    if backend == "memory":
        return InMemoryCausalStore()
    if backend == "sqlite":
        if database is None:
            raise ValueError("SQLite backend requires a database path")
        return SQLiteCausalStore(str(database))
    raise ValueError(f"Unsupported benchmark backend: {backend}")


def _init_worker(store: Any, benchmark_run_id: str, contention_groups: int) -> None:
    _thread_state.store = store
    _thread_state.benchmark_run_id = benchmark_run_id
    _thread_state.contention_groups = contention_groups


def _execute(index: int) -> tuple[float, str | None]:
    run_group = index % _thread_state.contention_groups
    runtime = ContinuityRuntime(
        store=_thread_state.store,
        run_id=f"{_thread_state.benchmark_run_id}-{run_group}",
    )
    started = time.perf_counter()
    try:
        runtime.step(lambda item: item, index, step_id=f"step-{index}")
    except Exception as exc:  # Report failures as measurements; don't disguise overload.
        return (time.perf_counter() - started, f"{type(exc).__name__}: {exc}")
    return (time.perf_counter() - started, None)


def _run_recovery_samples(store: Any, count: int, benchmark_run_id: str) -> dict[str, Any]:
    latencies_ms: list[float] = []
    failures = 0
    for index in range(count):
        runtime = ContinuityRuntime(store=store, run_id=f"{benchmark_run_id}-recovery-{index}")
        attempts = 0

        def fail_once() -> str:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("benchmark injected interruption")
            return "recovered"

        try:
            try:
                runtime.step(fail_once, step_id="recoverable-step")
            except RuntimeError:
                pass
            started = time.perf_counter()
            result = runtime.step(fail_once, step_id="recoverable-step")
            latencies_ms.append((time.perf_counter() - started) * 1000.0)
            if result != "recovered":
                failures += 1
        except Exception:
            failures += 1
    return {
        "samples_configured": count,
        "samples_completed": len(latencies_ms),
        "failures": failures,
        "latency_ms": {
            "p50": _percentile(latencies_ms, 50),
            "p95": _percentile(latencies_ms, 95),
            "p99": _percentile(latencies_ms, 99),
            "mean": statistics.fmean(latencies_ms) if latencies_ms else None,
        },
    }


def run(config: Config) -> dict[str, Any]:
    if config.workers < 1 or config.runs < 1 or config.recovery_samples < 0 or config.contention_groups < 1:
        raise ValueError("workers, runs, and contention-groups must be positive; recovery-samples cannot be negative")
    database = config.database
    if config.backend == "sqlite" and database is None:
        raise ValueError("Choose --database for SQLite or use --backend memory")

    if config.backend == "sqlite" and database is not None:
        database.parent.mkdir(parents=True, exist_ok=True)

    store = _create_store(config.backend, database)
    benchmark_run_id = f"benchmark-{time.time_ns()}"
    cpu_started = time.process_time()
    wall_started = time.perf_counter()
    latencies_s: list[float] = []
    failures: list[str] = []
    completed = 0
    tracemalloc.start()
    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=config.workers,
            initializer=_init_worker,
            initargs=(store, benchmark_run_id, config.contention_groups),
        ) as executor:
            remaining = iter(range(config.runs))
            pending: set[concurrent.futures.Future[tuple[float, str | None]]] = set()
            for _ in range(min(config.runs, config.workers * 2)):
                pending.add(executor.submit(_execute, next(remaining)))
            while pending:
                done, pending = concurrent.futures.wait(
                    pending, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for future in done:
                    latency, error = future.result()
                    latencies_s.append(latency)
                    if error:
                        failures.append(error)
                    else:
                        completed += 1
                    try:
                        pending.add(executor.submit(_execute, next(remaining)))
                    except StopIteration:
                        pass
        workload_elapsed = time.perf_counter() - wall_started
        recovery = _run_recovery_samples(store, config.recovery_samples, benchmark_run_id)
        current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    final_elapsed = time.perf_counter() - wall_started
    cpu_seconds = time.process_time() - cpu_started
    sqlite_database_bytes = database.stat().st_size if database is not None and database.exists() else None
    sqlite_wal = Path(f"{database}-wal") if database is not None else None
    sqlite_shm = Path(f"{database}-shm") if database is not None else None
    sqlite_wal_bytes = sqlite_wal.stat().st_size if sqlite_wal is not None and sqlite_wal.exists() else 0
    sqlite_shm_bytes = sqlite_shm.stat().st_size if sqlite_shm is not None and sqlite_shm.exists() else 0
    sqlite_total_storage_bytes = (
        (sqlite_database_bytes or 0) + sqlite_wal_bytes + sqlite_shm_bytes if database is not None else None
    )
    return {
        "report_schema_version": 1,
        "status": "measured",
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "python": os.sys.version.split()[0],
            "platform": os.name,
            "cpu_count": os.cpu_count(),
        },
        "configuration": {
            "workers": config.workers,
            "runs_configured": config.runs,
            "backend": config.backend,
            "database": str(database) if database else None,
            "recovery_samples_configured": config.recovery_samples,
            "contention_groups": config.contention_groups,
            "scenario_source": str(SCENARIOS_PATH),
        },
        "results": {
            "runs_completed": completed,
            "failed_runs": len(failures),
            "first_failures": failures[:10],
            "workload_wall_seconds": workload_elapsed,
            "report_wall_seconds_including_recovery": final_elapsed,
            "throughput_runs_per_second": completed / workload_elapsed if workload_elapsed else None,
            "latency_ms": {
                "p50": _percentile([v * 1000 for v in latencies_s], 50),
                "p95": _percentile([v * 1000 for v in latencies_s], 95),
                "p99": _percentile([v * 1000 for v in latencies_s], 99),
                "mean": statistics.fmean(latencies_s) * 1000 if latencies_s else None,
            },
            "process_cpu_seconds": cpu_seconds,
            "process_cpu_percent_of_one_core": (cpu_seconds / final_elapsed * 100) if final_elapsed else None,
            "tracemalloc_current_bytes": current_bytes,
            "tracemalloc_peak_bytes": peak_bytes,
            "sqlite_database_bytes": sqlite_database_bytes,
            "sqlite_wal_bytes": sqlite_wal_bytes if database is not None else None,
            "sqlite_shm_bytes": sqlite_shm_bytes if database is not None else None,
            "sqlite_total_storage_bytes": sqlite_total_storage_bytes,
            "recovery": recovery,
        },
        "interpretation": "This is a local microbenchmark measurement, not a production capacity guarantee.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=sorted(_scenario_map()), help="Use a configured workers/runs scenario")
    parser.add_argument("--workers", type=int, default=8, help="Worker threads; default is a smoke-sized 8")
    parser.add_argument("--runs", type=int, default=1000, help="Workflow runs; default is a smoke-sized 1,000")
    parser.add_argument("--backend", choices=("memory", "sqlite"), default="sqlite")
    parser.add_argument("--database", type=Path, help="SQLite path; omitted creates a temporary database")
    parser.add_argument("--recovery-samples", type=int, default=25)
    parser.add_argument(
        "--contention-groups",
        type=int,
        help="Number of shared run histories; default equals workers. Lower values increase append contention and history size.",
    )
    parser.add_argument("--output", type=Path, help="Write JSON report to this path (stdout otherwise)")
    args = parser.parse_args()

    workers, runs = args.workers, args.runs
    if args.scenario:
        scenario = _scenario_map()[args.scenario]
        workers, runs = scenario["workers"], scenario["runs"]
    database = args.database
    if args.backend == "sqlite" and database is None:
        temp = tempfile.NamedTemporaryFile(prefix="continuity-benchmark-", suffix=".db", delete=False)
        temp.close()
        database = Path(temp.name)
    contention_groups = args.contention_groups or (scenario.get("contention_groups", workers) if args.scenario else workers)
    report = run(Config(workers, runs, args.backend, database, args.recovery_samples, contention_groups))
    report["configuration"]["scenario_id"] = args.scenario
    serialized = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized + "\n", encoding="utf-8")
    else:
        print(serialized)
    recovery = report["results"]["recovery"]
    return 0 if report["results"]["failed_runs"] == 0 and recovery["failures"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
