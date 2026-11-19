from __future__ import annotations
import json
import math
import os
import random
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Protocol, runtime_checkable
from ..models import CausalEvent, safe_json_default


@runtime_checkable
class CausalStoreProtocol(Protocol):
    def append_event(self, event: CausalEvent, lease: Optional["StepLease"] = None) -> CausalEvent:
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

    def acquire_fenced_step_lease(
        self, run_id: str, step_id: str, worker_id: str, ttl_seconds: float = 30.0
    ) -> Optional["StepLease"]:
        ...

    def renew_fenced_step_lease(self, lease: "StepLease", ttl_seconds: float = 30.0) -> Optional["StepLease"]:
        ...

    def release_fenced_step_lease(self, lease: "StepLease") -> None:
        ...

    def list_runs(self) -> List[str]:
        """Return all known run IDs."""
        ...


class OptimisticConcurrencyError(RuntimeError):
    """Raised when an append conflicts on sequence number or active step lease."""


class FencingTokenError(RuntimeError):
    """Raised when an event append presents an expired or superseded lease token."""


def validate_lease_ttl(ttl_seconds: float) -> None:
    if not math.isfinite(ttl_seconds) or ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be finite and positive")


@dataclass(frozen=True)
class StepLease:
    run_id: str
    step_id: str
    owner_id: str
    fencing_token: int


class InMemoryCausalStore:
    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._lock = threading.RLock()
        self._events: Dict[str, List[CausalEvent]] = {}
        self._leases: Dict[tuple[str, str], tuple[str, Optional[float], int]] = {}
        self._fencing_counters: Dict[tuple[str, str], int] = {}
        self._clock = clock

    def append_event(self, event: CausalEvent, lease: Optional[StepLease] = None) -> CausalEvent:
        with self._lock:
            if lease is not None:
                current = self._leases.get((lease.run_id, lease.step_id))
                if (
                    lease.run_id != event.run_id
                    or lease.step_id != event.step_id
                    or current is None
                    or current[0] != lease.owner_id
                    or current[2] != lease.fencing_token
                    or current[1] is None
                    or current[1] <= self._clock()
                ):
                    raise FencingTokenError("Lease owner or fencing token is stale or expired")
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
        return self.acquire_fenced_step_lease(run_id, step_id, worker_id, ttl_seconds) is not None

    def acquire_fenced_step_lease(
        self, run_id: str, step_id: str, worker_id: str, ttl_seconds: float = 30.0
    ) -> Optional[StepLease]:
        validate_lease_ttl(ttl_seconds)
        with self._lock:
            now = self._clock()
            key = (run_id, step_id)
            existing = self._leases.get(key)
            if existing is not None:
                holder, expiry, token = existing
                if expiry is not None and expiry > now:
                    if holder != worker_id:
                        return None
                    self._leases[key] = (worker_id, now + ttl_seconds, token)
                    return StepLease(run_id, step_id, worker_id, token)
            token = self._fencing_counters.get(key, 0) + 1
            self._fencing_counters[key] = token
            self._leases[key] = (worker_id, now + ttl_seconds, token)
            return StepLease(run_id, step_id, worker_id, token)

    def renew_fenced_step_lease(self, lease: StepLease, ttl_seconds: float = 30.0) -> Optional[StepLease]:
        validate_lease_ttl(ttl_seconds)
        with self._lock:
            now = self._clock()
            key = (lease.run_id, lease.step_id)
            current = self._leases.get(key)
            if (
                current is None
                or current[0] != lease.owner_id
                or current[2] != lease.fencing_token
                or current[1] is None
                or current[1] <= now
            ):
                return None
            self._leases[key] = (lease.owner_id, now + ttl_seconds, lease.fencing_token)
            return lease

    def release_fenced_step_lease(self, lease: StepLease) -> None:
        with self._lock:
            key = (lease.run_id, lease.step_id)
            current = self._leases.get(key)
            if current is not None and current[0] == lease.owner_id and current[2] == lease.fencing_token:
                self._leases[key] = (lease.owner_id, None, lease.fencing_token)

    def release_step_lease(self, run_id: str, step_id: str, worker_id: str) -> None:
        with self._lock:
            key = (run_id, step_id)
            existing = self._leases.get(key)
            if existing and existing[0] == worker_id:
                self._leases[key] = (existing[0], None, existing[2])

    def list_runs(self) -> List[str]:
        with self._lock:
            return list(self._events.keys())


class SQLiteCausalStore:
    """Crash-safe SQLite WAL causal store designed for multi-process & os._exit resilience."""

    def __init__(self, db_path: str, clock: Callable[[], float] = time.time) -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        self._clock = clock
        parent = os.path.dirname(os.path.abspath(db_path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.execute("PRAGMA synchronous=FULL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn

    @staticmethod
    def _enable_wal(conn: sqlite3.Connection) -> None:
        deadline = time.monotonic() + 30.0
        retry_delay = 0.005
        while True:
            try:
                mode = conn.execute("PRAGMA journal_mode=WAL;").fetchone()
                if mode is None or mode[0].lower() != "wal":
                    raise RuntimeError("SQLite did not enable WAL journal mode")
                return
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if ("locked" not in message and "busy" not in message) or time.monotonic() >= deadline:
                    raise
                time.sleep(random.uniform(retry_delay, min(retry_delay * 2, 0.1)))
                retry_delay = min(retry_delay * 2, 0.1)

    def _init_db(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                self._enable_wal(conn)
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
                        schema_version INTEGER NOT NULL DEFAULT 1,
                        PRIMARY KEY (run_id, seq)
                    );
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS step_leases (
                        run_id TEXT NOT NULL,
                        step_id TEXT NOT NULL,
                        worker_id TEXT NOT NULL,
                        expires_at REAL NOT NULL,
                        fencing_token INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (run_id, step_id)
                    );
                """)
                event_columns = {row[1] for row in conn.execute("PRAGMA table_info(causal_events)")}
                if "schema_version" not in event_columns:
                    conn.execute("ALTER TABLE causal_events ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1")
                lease_columns = {row[1] for row in conn.execute("PRAGMA table_info(step_leases)")}
                if "fencing_token" not in lease_columns:
                    conn.execute("ALTER TABLE step_leases ADD COLUMN fencing_token INTEGER NOT NULL DEFAULT 0")
                conn.execute("COMMIT;")
            except Exception:
                if conn.in_transaction:
                    conn.execute("ROLLBACK;")
                raise
            finally:
                conn.close()

    def append_event(self, event: CausalEvent, lease: Optional[StepLease] = None) -> CausalEvent:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                if lease is not None:
                    row = conn.execute(
                        "SELECT worker_id, expires_at, fencing_token FROM step_leases WHERE run_id = ? AND step_id = ?",
                        (lease.run_id, lease.step_id),
                    ).fetchone()
                    if (
                        lease.run_id != event.run_id
                        or lease.step_id != event.step_id
                        or row is None
                        or row[0] != lease.owner_id
                        or int(row[2]) != lease.fencing_token
                        or float(row[1]) <= self._clock()
                    ):
                        raise FencingTokenError("Lease owner or fencing token is stale or expired")
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
                        timestamp, worker_id, attempt, schema_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        event.schema_version,
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
                       timestamp, worker_id, attempt, schema_version
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
                            "schema_version": r[13],
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
        return self.acquire_fenced_step_lease(run_id, step_id, worker_id, ttl_seconds) is not None

    def acquire_fenced_step_lease(
        self, run_id: str, step_id: str, worker_id: str, ttl_seconds: float = 30.0
    ) -> Optional[StepLease]:
        validate_lease_ttl(ttl_seconds)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                now = self._clock()
                row = conn.execute(
                    "SELECT worker_id, expires_at, fencing_token FROM step_leases WHERE run_id = ? AND step_id = ?",
                    (run_id, step_id),
                ).fetchone()
                if row is not None:
                    holder, expiry, prior_token = row[0], float(row[1]), int(row[2])
                    if expiry > now and holder != worker_id:
                        conn.execute("ROLLBACK;")
                        return None
                    token = (prior_token or 1) if expiry > now and holder == worker_id else prior_token + 1
                else:
                    token = 1
                conn.execute(
                    """
                    INSERT INTO step_leases (run_id, step_id, worker_id, expires_at, fencing_token)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, step_id) DO UPDATE SET
                        worker_id = excluded.worker_id,
                        expires_at = excluded.expires_at,
                        fencing_token = excluded.fencing_token
                    """,
                    (run_id, step_id, worker_id, now + ttl_seconds, token),
                )
                conn.execute("COMMIT;")
                return StepLease(run_id, step_id, worker_id, token)
            except Exception:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                raise
            finally:
                conn.close()

    def renew_fenced_step_lease(self, lease: StepLease, ttl_seconds: float = 30.0) -> Optional[StepLease]:
        validate_lease_ttl(ttl_seconds)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                now = self._clock()
                cursor = conn.execute(
                    """UPDATE step_leases SET expires_at = ?
                       WHERE run_id = ? AND step_id = ? AND worker_id = ?
                         AND fencing_token = ? AND expires_at > ?""",
                    (now + ttl_seconds, lease.run_id, lease.step_id, lease.owner_id, lease.fencing_token, now),
                )
                conn.execute("COMMIT;")
                return lease if cursor.rowcount == 1 else None
            except Exception:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                raise
            finally:
                conn.close()

    def release_fenced_step_lease(self, lease: StepLease) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                conn.execute(
                    """UPDATE step_leases SET expires_at = ?
                       WHERE run_id = ? AND step_id = ? AND worker_id = ? AND fencing_token = ?""",
                    (float("-inf"), lease.run_id, lease.step_id, lease.owner_id, lease.fencing_token),
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

    def release_step_lease(self, run_id: str, step_id: str, worker_id: str) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                conn.execute(
                    "UPDATE step_leases SET expires_at = ? WHERE run_id = ? AND step_id = ? AND worker_id = ?",
                    (float("-inf"), run_id, step_id, worker_id),
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
