from __future__ import annotations
import json
import os
import sqlite3
import threading
import time
from typing import Dict, List, Protocol, runtime_checkable
from ..models import CausalEvent, safe_json_default


@runtime_checkable
class CausalStoreProtocol(Protocol):
    def append_event(self, event: CausalEvent) -> CausalEvent:
        """Atomically append an event enforcing monotonic per-run sequence numbers."""
        ...

    def get_events(self, run_id: str) -> List[CausalEvent]:
        """Return all causal events for a run ordered by seq ascending."""
        ...

    def get_latest_seq(self, run_id: str) -> int:
        """Return highest committed seq for run_id, or 0 if empty."""
        ...

    def acquire_step_lease(self, run_id: str, step_id: str, worker_id: str, ttl_seconds: float = 30.0) -> bool:
        """Acquire exclusive execution lease for a step across concurrent workers."""
        ...

    def release_step_lease(self, run_id: str, step_id: str, worker_id: str) -> None:
        """Release lease held by worker_id."""
        ...

    def list_runs(self) -> List[str]:
        """Return all known run IDs."""
        ...


class OptimisticConcurrencyError(RuntimeError):
    """Raised when an append conflicts on sequence number or active step lease."""


class InMemoryCausalStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._events: Dict[str, List[CausalEvent]] = {}
        self._leases: Dict[tuple[str, str], tuple[str, float]] = {}

    def append_event(self, event: CausalEvent) -> CausalEvent:
        with self._lock:
            run_events = self._events.setdefault(event.run_id, [])
            expected_seq = len(run_events) + 1
            if event.seq != expected_seq:
                raise OptimisticConcurrencyError(
                    f"Sequence mismatch for run {event.run_id}: got {event.seq}, expected {expected_seq}"
                )
            run_events.append(event)
            return event

    def get_events(self, run_id: str) -> List[CausalEvent]:
        with self._lock:
            return list(self._events.get(run_id, []))

    def get_latest_seq(self, run_id: str) -> int:
        with self._lock:
            return len(self._events.get(run_id, []))

    def acquire_step_lease(self, run_id: str, step_id: str, worker_id: str, ttl_seconds: float = 30.0) -> bool:
        now = time.time()
        with self._lock:
            key = (run_id, step_id)
            existing = self._leases.get(key)
            if existing is not None:
                holder, expiry = existing
                if holder != worker_id and expiry > now:
                    return False
            self._leases[key] = (worker_id, now + ttl_seconds)
            return True

    def release_step_lease(self, run_id: str, step_id: str, worker_id: str) -> None:
        with self._lock:
            key = (run_id, step_id)
            existing = self._leases.get(key)
            if existing and existing[0] == worker_id:
                del self._leases[key]

    def list_runs(self) -> List[str]:
        with self._lock:
            return list(self._events.keys())


class SQLiteCausalStore:
    """Crash-safe SQLite WAL causal store designed for multi-process & os._exit resilience."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        parent = os.path.dirname(os.path.abspath(db_path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=FULL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn

    def _init_db(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS causal_events (
                        run_id TEXT NOT NULL,
                        seq INTEGER NOT NULL,
                        event_id TEXT NOT NULL UNIQUE,
                        step_id TEXT NOT NULL,
                        step_name TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        side_effect TEXT NOT NULL,
                        idempotency_key TEXT NOT NULL,
                        parent_event_ids TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        timestamp REAL NOT NULL,
                        worker_id TEXT NOT NULL,
                        attempt INTEGER NOT NULL,
                        PRIMARY KEY (run_id, seq)
                    );
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS step_leases (
                        run_id TEXT NOT NULL,
                        step_id TEXT NOT NULL,
                        worker_id TEXT NOT NULL,
                        expires_at REAL NOT NULL,
                        PRIMARY KEY (run_id, step_id)
                    );
                """)
                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise
            finally:
                conn.close()

    def append_event(self, event: CausalEvent) -> CausalEvent:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                row = conn.execute(
                    "SELECT COALESCE(MAX(seq), 0) FROM causal_events WHERE run_id = ?",
                    (event.run_id,),
                ).fetchone()
                latest_seq = int(row[0]) if row else 0
                expected_seq = latest_seq + 1
                if event.seq != expected_seq:
                    conn.execute("ROLLBACK;")
                    raise OptimisticConcurrencyError(
                        f"Sequence mismatch for run {event.run_id}: got {event.seq}, expected {expected_seq}"
                    )
                conn.execute(
                    """
                    INSERT INTO causal_events (
                        run_id, seq, event_id, step_id, step_name, event_type,
                        side_effect, idempotency_key, parent_event_ids, payload,
                        timestamp, worker_id, attempt
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.run_id,
                        event.seq,
                        event.event_id,
                        event.step_id,
                        event.step_name,
                        event.event_type.value,
                        event.side_effect.value,
                        event.idempotency_key,
                        json.dumps(event.parent_event_ids, default=safe_json_default),
                        json.dumps(event.payload, default=safe_json_default),
                        event.timestamp,
                        event.worker_id,
                        event.attempt,
                    ),
                )
                conn.execute("COMMIT;")
                return event
            except OptimisticConcurrencyError:
                raise
            except sqlite3.IntegrityError as exc:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                raise OptimisticConcurrencyError(str(exc)) from exc
            except Exception:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                raise
            finally:
                conn.close()

    def get_events(self, run_id: str) -> List[CausalEvent]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT run_id, seq, event_id, step_id, step_name, event_type,
                       side_effect, idempotency_key, parent_event_ids, payload,
                       timestamp, worker_id, attempt
                FROM causal_events
                WHERE run_id = ?
                ORDER BY seq ASC
                """,
                (run_id,),
            ).fetchall()
            events: List[CausalEvent] = []
            for r in rows:
                events.append(
                    CausalEvent.from_dict(
                        {
                            "run_id": r[0],
                            "seq": r[1],
                            "event_id": r[2],
                            "step_id": r[3],
                            "step_name": r[4],
                            "event_type": r[5],
                            "side_effect": r[6],
                            "idempotency_key": r[7],
                            "parent_event_ids": json.loads(r[8]),
                            "payload": json.loads(r[9]),
                            "timestamp": r[10],
                            "worker_id": r[11],
                            "attempt": r[12],
                        }
                    )
                )
            return events
        finally:
            conn.close()

    def get_latest_seq(self, run_id: str) -> int:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) FROM causal_events WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()

    def acquire_step_lease(self, run_id: str, step_id: str, worker_id: str, ttl_seconds: float = 30.0) -> bool:
        now = time.time()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                row = conn.execute(
                    "SELECT worker_id, expires_at FROM step_leases WHERE run_id = ? AND step_id = ?",
                    (run_id, step_id),
                ).fetchone()
                if row is not None:
                    holder, expiry = row[0], float(row[1])
                    if holder != worker_id and expiry > now:
                        conn.execute("ROLLBACK;")
                        return False
                conn.execute(
                    """
                    INSERT INTO step_leases (run_id, step_id, worker_id, expires_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(run_id, step_id) DO UPDATE SET
                        worker_id = excluded.worker_id,
                        expires_at = excluded.expires_at
                    """,
                    (run_id, step_id, worker_id, now + ttl_seconds),
                )
                conn.execute("COMMIT;")
                return True
            except Exception:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                raise
            finally:
                conn.close()

    def release_step_lease(self, run_id: str, step_id: str, worker_id: str) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                conn.execute(
                    "DELETE FROM step_leases WHERE run_id = ? AND step_id = ? AND worker_id = ?",
                    (run_id, step_id, worker_id),
                )
                conn.execute("COMMIT;")
            except Exception:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                raise
            finally:
                conn.close()

    def list_runs(self) -> List[str]:
        conn = self._connect()
        try:
            rows = conn.execute("SELECT DISTINCT run_id FROM causal_events ORDER BY run_id ASC").fetchall()
            return [r[0] for r in rows]
        finally:
            conn.close()
