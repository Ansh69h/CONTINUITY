"""Configurable long-running durability/resource soak harness.

This harness defaults to the 1-hour profile. Short runs may be supplied explicitly
with --duration-seconds for smoke checks; those are not 1/6/24-hour soak results.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import signal
import threading
import time
import tracemalloc
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from continuity import ContinuityRuntime, InMemoryCausalStore, SQLiteCausalStore


class SoakRun:
    def __init__(self, backend: str, workers: int, db_path: Path | None):
        self.backend = backend
        self.workers = workers
        self.db_path = db_path
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.completed = 0
        self.failure_count = 0
        self.failures: list[str] = []
        self.started_at = time.perf_counter()
        self.cpu_started = time.process_time()

    def _store(self) -> Any:
        if self.backend == "memory":
            return InMemoryCausalStore()
        if self.db_path is None:
            raise ValueError("SQLite soak requires a database path")
        return SQLiteCausalStore(str(self.db_path))

    def worker(self, worker_index: int) -> None:
        store = self._store()
        operation = 0
        while not self.stop_event.is_set():
            operation += 1
            runtime = ContinuityRuntime(
                store=store,
                run_id=f"soak-{worker_index}-{operation}",
                worker_id=f"soak-worker-{worker_index}",
                lease_ttl=30.0,
            )
            try:
                runtime.step(lambda value: value + 1, operation, step_id="increment")
                with self.lock:
                    self.completed += 1
            except Exception as exc:
                with self.lock:
                    self.failure_count += 1
                    if len(self.failures) < 10:
                        self.failures.append(f"{type(exc).__name__}: {exc}")

    def stop(self) -> None:
        self.stop_event.set()

    def snapshot(self) -> dict[str, Any]:
        current, peak = tracemalloc.get_traced_memory()
        elapsed = time.perf_counter() - self.started_at
        sqlite_database_bytes = self.db_path.stat().st_size if self.db_path and self.db_path.exists() else None
        sqlite_wal = Path(f"{self.db_path}-wal") if self.db_path else None
        sqlite_shm = Path(f"{self.db_path}-shm") if self.db_path else None
        sqlite_wal_bytes = sqlite_wal.stat().st_size if sqlite_wal is not None and sqlite_wal.exists() else 0
        sqlite_shm_bytes = sqlite_shm.stat().st_size if sqlite_shm is not None and sqlite_shm.exists() else 0
        return {
            "elapsed_seconds": round(elapsed, 3),
            "completed_workflows": self.completed,
            "failed_workflows": self.failure_count,
            "first_failures": self.failures[:10],
            "throughput_workflows_per_second": self.completed / elapsed if elapsed else None,
            "process_cpu_seconds": time.process_time() - self.cpu_started,
            "tracemalloc_current_bytes": current,
            "tracemalloc_peak_bytes": peak,
            "sqlite_database_bytes": sqlite_database_bytes,
            "sqlite_wal_bytes": sqlite_wal_bytes if self.db_path else None,
            "sqlite_shm_bytes": sqlite_shm_bytes if self.db_path else None,
            "sqlite_total_storage_bytes": (
                (sqlite_database_bytes or 0) + sqlite_wal_bytes + sqlite_shm_bytes if self.db_path else None
            ),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-hours", type=int, choices=(1, 6, 24), default=1)
    parser.add_argument("--duration-seconds", type=float, help="Short smoke run override; not a formal soak duration")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--backend", choices=("memory", "sqlite"), default="sqlite")
    parser.add_argument("--database", type=Path, default=Path(".continuity-soak.db"))
    parser.add_argument("--report-interval-seconds", type=float, default=60)
    parser.add_argument("--output", type=Path, default=Path("soak-report.jsonl"))
    args = parser.parse_args()
    if args.workers < 1 or args.report_interval_seconds <= 0:
        parser.error("workers and report interval must be positive")
    if args.duration_seconds is not None and args.duration_seconds <= 0:
        parser.error("duration-seconds must be positive")

    duration_seconds = args.duration_seconds if args.duration_seconds is not None else args.duration_hours * 3600
    if args.backend == "sqlite":
        try:
            _validate_report_path(args.database, args.output)
        except ValueError as exc:
            parser.error(str(exc))
    run = SoakRun(args.backend, args.workers, args.database if args.backend == "sqlite" else None)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    stop_signals = []

    def stop_handler(signum, _frame):
        stop_signals.append(signum)
        run.stop()

    old_handlers = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            old_handlers[sig] = signal.signal(sig, stop_handler)
        except (ValueError, OSError):
            pass

    tracemalloc.start()
    actual_started = datetime.now(timezone.utc)
    deadline = time.perf_counter() + duration_seconds
    profile = f"{args.duration_hours}h" if args.duration_seconds is None else "custom-short-run"
    header = {
        "record_schema_version": 1,
        "record_type": "soak_header",
        "configured_profile": profile,
        "configured_duration_hours": args.duration_hours if args.duration_seconds is None else None,
        "configured_duration_seconds": duration_seconds,
        "workers": args.workers,
        "backend": args.backend,
        "started_at_utc": actual_started.isoformat(),
        "status": "running",
    }

    try:
        with args.output.open("w", encoding="utf-8") as report_file:
            report_file.write(json.dumps(header) + "\n")
            report_file.flush()
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [executor.submit(run.worker, index) for index in range(args.workers)]
                try:
                    while time.perf_counter() < deadline:
                        remaining = deadline - time.perf_counter()
                        if run.stop_event.wait(min(args.report_interval_seconds, remaining)):
                            break
                        if time.perf_counter() >= deadline:
                            break
                        checkpoint = {
                            "record_schema_version": 1,
                            "record_type": "resource_checkpoint",
                            **run.snapshot(),
                        }
                        report_file.write(json.dumps(checkpoint) + "\n")
                        report_file.flush()
                finally:
                    run.stop()
                for future in futures:
                    future.result()
            status = "interrupted" if stop_signals else "completed"
            footer = {
                "record_schema_version": 1,
                "record_type": "soak_summary",
                "status": status,
                "actual_duration_seconds": round(time.perf_counter() - run.started_at, 3),
                "finished_at_utc": datetime.now(timezone.utc).isoformat(),
                **run.snapshot(),
            }
            report_file.write(json.dumps(footer) + "\n")
    finally:
        tracemalloc.stop()
        for sig, old_handler in old_handlers.items():
            signal.signal(sig, old_handler)

    return 0 if not run.failure_count and not stop_signals else 1


def _validate_report_path(database: Path, output: Path) -> None:
    resolved_output = output.expanduser().resolve(strict=False)
    resolved_database = database.expanduser().resolve(strict=False)
    protected = [resolved_database, *(Path(f"{resolved_database}{suffix}") for suffix in ("-wal", "-shm", "-journal"))]
    output_key = os.path.normcase(str(resolved_output))
    for protected_path in protected:
        if os.path.normcase(str(protected_path)) == output_key:
            raise ValueError("output report path must not be the SQLite database or one of its sidecar files")
        if protected_path.exists() and resolved_output.exists() and os.path.samefile(protected_path, resolved_output):
            raise ValueError("output report path must not be the SQLite database or one of its sidecar files")


if __name__ == "__main__":
    raise SystemExit(main())
