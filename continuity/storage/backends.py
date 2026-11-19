from __future__ import annotations
import json
import random
import re
import time
import base64
import uuid
from typing import Any, List
from ..models import CausalEvent, safe_json_default
from .base import FencingTokenError, OptimisticConcurrencyError, StepLease, validate_lease_ttl


class PostgresCausalStore:
    """Optional PostgreSQL store using transactional DB-API 2.0 / psycopg connections."""

    def __init__(self, conn_factory: Any, table_prefix: str = "continuity") -> None:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_prefix):
            raise ValueError("table_prefix must be a simple SQL identifier prefix")
        self._conn_factory = conn_factory
        self._events_table = f"{table_prefix}_causal_events"
        self._leases_table = f"{table_prefix}_step_leases"

    def ensure_schema(self) -> None:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self._events_table} (
                        run_id TEXT NOT NULL,
                        seq BIGINT NOT NULL,
                        event_id TEXT NOT NULL UNIQUE,
                        step_id TEXT NOT NULL,
                        step_name TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        side_effect TEXT NOT NULL,
                        idempotency_key TEXT NOT NULL,
                        parent_event_ids JSONB NOT NULL,
                        payload JSONB NOT NULL,
                        timestamp DOUBLE PRECISION NOT NULL,
                        worker_id TEXT NOT NULL,
                        attempt INTEGER NOT NULL,
                        schema_version INTEGER NOT NULL DEFAULT 1,
                        PRIMARY KEY (run_id, seq)
                    );
                """)
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self._leases_table} (
                        run_id TEXT NOT NULL,
                        step_id TEXT NOT NULL,
                        worker_id TEXT NOT NULL,
                        expires_at DOUBLE PRECISION NOT NULL,
                        fencing_token BIGINT NOT NULL DEFAULT 0,
                        PRIMARY KEY (run_id, step_id)
                    );
                """)
                cur.execute(
                    f"ALTER TABLE {self._events_table} ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 1"
                )
                cur.execute(
                    f"ALTER TABLE {self._leases_table} ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0"
                )
            conn.commit()

    def append_event(self, event: CausalEvent, lease: StepLease | None = None) -> CausalEvent:
        with self._conn_factory() as conn:
            if getattr(conn, "autocommit", False):
                raise ValueError("PostgreSQL append_event requires autocommit=False to hold checks through insert")
            with conn.cursor() as cur:
                if lease is not None:
                    if lease.run_id != event.run_id or lease.step_id != event.step_id:
                        raise FencingTokenError("Lease does not authorize this event")
                    cur.execute(
                        f"""SELECT worker_id, fencing_token, expires_at
                            FROM {self._leases_table}
                            WHERE run_id = %s AND step_id = %s
                            FOR UPDATE""",
                        (lease.run_id, lease.step_id),
                    )
                    lease_row = cur.fetchone()
                    if lease_row is None or lease_row[0] != lease.owner_id or int(lease_row[1]) != lease.fencing_token:
                        raise FencingTokenError("Lease owner or fencing token is stale or expired")
                    cur.execute("SELECT EXTRACT(EPOCH FROM clock_timestamp())")
                    if float(lease_row[2]) <= float(cur.fetchone()[0]):
                        raise FencingTokenError("Lease owner or fencing token is stale or expired")
                cur.execute(
                    f"SELECT COALESCE(MAX(seq), 0) FROM {self._events_table} WHERE run_id = %s",
                    (event.run_id,),
                )
                latest = int(cur.fetchone()[0])
                if event.seq != latest + 1:
                    raise OptimisticConcurrencyError(
                        f"Sequence mismatch for {event.run_id}: got {event.seq}, expected {latest + 1}"
                    )
                try:
                    cur.execute(
                        f"""
                        INSERT INTO {self._events_table} (
                            run_id, seq, event_id, step_id, step_name, event_type,
                            side_effect, idempotency_key, parent_event_ids, payload,
                            timestamp, worker_id, attempt, schema_version
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                except Exception as exc:
                    sqlstate = getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)
                    if sqlstate == "23505":
                        raise OptimisticConcurrencyError(str(exc)) from exc
                    raise
            conn.commit()
        return event

    def get_events(self, run_id: str) -> List[CausalEvent]:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT run_id, seq, event_id, step_id, step_name, event_type,
                           side_effect, idempotency_key, parent_event_ids, payload,
                           timestamp, worker_id, attempt, schema_version
                    FROM {self._events_table}
                    WHERE run_id = %s ORDER BY seq ASC
                    """,
                    (run_id,),
                )
                rows = cur.fetchall()
        results: List[CausalEvent] = []
        for r in rows:
            parents = r[8] if isinstance(r[8], list) else json.loads(r[8])
            payload = r[9] if isinstance(r[9], dict) else json.loads(r[9])
            results.append(
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
                        "parent_event_ids": parents,
                        "payload": payload,
                        "timestamp": r[10],
                        "worker_id": r[11],
                        "attempt": r[12],
                        "schema_version": r[13],
                    }
                )
            )
        return results

    def get_latest_seq(self, run_id: str) -> int:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COALESCE(MAX(seq), 0) FROM {self._events_table} WHERE run_id = %s",
                    (run_id,),
                )
                return int(cur.fetchone()[0])

    def acquire_step_lease(self, run_id: str, step_id: str, worker_id: str, ttl_seconds: float = 30.0) -> bool:
        return self.acquire_fenced_step_lease(run_id, step_id, worker_id, ttl_seconds) is not None

    def acquire_fenced_step_lease(
        self, run_id: str, step_id: str, worker_id: str, ttl_seconds: float = 30.0
    ) -> StepLease | None:
        validate_lease_ttl(ttl_seconds)
        with self._conn_factory() as conn:
            if getattr(conn, "autocommit", False):
                raise ValueError("PostgreSQL lease acquisition requires autocommit=False")
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._leases_table} (run_id, step_id, worker_id, expires_at, fencing_token)
                    VALUES (%s, %s, %s, EXTRACT(EPOCH FROM clock_timestamp()) + %s, 1)
                    ON CONFLICT (run_id, step_id) DO UPDATE SET
                        worker_id = EXCLUDED.worker_id,
                        expires_at = EXTRACT(EPOCH FROM clock_timestamp()) + %s,
                        fencing_token = CASE
                            WHEN {self._leases_table}.expires_at <= EXTRACT(EPOCH FROM clock_timestamp())
                            THEN {self._leases_table}.fencing_token + 1
                            WHEN {self._leases_table}.fencing_token = 0 THEN 1
                            ELSE {self._leases_table}.fencing_token
                        END
                    WHERE {self._leases_table}.expires_at <= EXTRACT(EPOCH FROM clock_timestamp())
                       OR {self._leases_table}.worker_id = EXCLUDED.worker_id
                    RETURNING worker_id, fencing_token
                    """,
                    (
                        run_id, step_id, worker_id, ttl_seconds, ttl_seconds,
                    ),
                )
                row = cur.fetchone()
                if row is not None:
                    cur.execute(
                        f"""UPDATE {self._leases_table}
                            SET expires_at = EXTRACT(EPOCH FROM clock_timestamp()) + %s
                            WHERE run_id = %s AND step_id = %s AND worker_id = %s AND fencing_token = %s
                            RETURNING worker_id, fencing_token""",
                        (ttl_seconds, run_id, step_id, worker_id, int(row[1])),
                    )
                    row = cur.fetchone()
            conn.commit()
        return StepLease(run_id, step_id, worker_id, int(row[1])) if row is not None else None

    def renew_fenced_step_lease(self, lease: StepLease, ttl_seconds: float = 30.0) -> StepLease | None:
        validate_lease_ttl(ttl_seconds)
        with self._conn_factory() as conn:
            if getattr(conn, "autocommit", False):
                raise ValueError("PostgreSQL lease renewal requires autocommit=False")
            with conn.cursor() as cur:
                cur.execute(
                    f"""SELECT worker_id, fencing_token, expires_at
                        FROM {self._leases_table}
                        WHERE run_id = %s AND step_id = %s
                        FOR UPDATE""",
                    (lease.run_id, lease.step_id),
                )
                row = cur.fetchone()
                if row is not None and row[0] == lease.owner_id and int(row[1]) == lease.fencing_token:
                    cur.execute("SELECT EXTRACT(EPOCH FROM clock_timestamp())")
                    if float(row[2]) <= float(cur.fetchone()[0]):
                        row = None
                    else:
                        cur.execute(
                            f"""UPDATE {self._leases_table}
                                SET expires_at = EXTRACT(EPOCH FROM clock_timestamp()) + %s
                                WHERE run_id = %s AND step_id = %s AND worker_id = %s AND fencing_token = %s
                                RETURNING worker_id, fencing_token""",
                            (ttl_seconds, lease.run_id, lease.step_id, lease.owner_id, lease.fencing_token),
                        )
                        row = cur.fetchone()
                else:
                    row = None
            conn.commit()
        return lease if row is not None else None

    def release_fenced_step_lease(self, lease: StepLease) -> None:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE {self._leases_table} SET expires_at = '-Infinity'
                        WHERE run_id = %s AND step_id = %s AND worker_id = %s AND fencing_token = %s""",
                    (lease.run_id, lease.step_id, lease.owner_id, lease.fencing_token),
                )
            conn.commit()

    def release_step_lease(self, run_id: str, step_id: str, worker_id: str) -> None:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {self._leases_table} SET expires_at = '-Infinity' WHERE run_id = %s AND step_id = %s AND worker_id = %s",
                    (run_id, step_id, worker_id),
                )
            conn.commit()

    def list_runs(self) -> List[str]:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT DISTINCT run_id FROM {self._events_table} ORDER BY run_id ASC")
                return [r[0] for r in cur.fetchall()]


class RedisCausalStore:
    """Optional Redis distributed causal store using atomic Lua CAS scripts."""

    _APPEND_LUA = """
    local list_key = KEYS[1]
    local run_marker_key = KEYS[2]
    local expected_seq = tonumber(ARGV[1])
    local payload = ARGV[2]
    local run_id = ARGV[3]
    local current_len = redis.call('LLEN', list_key)
    if current_len + 1 ~= expected_seq then
        return -1
    end
    redis.call('RPUSH', list_key, payload)
    redis.call('SET', run_marker_key, '1')
    return expected_seq
    """

    _RENEW_LEASE_LUA = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        redis.call('PEXPIRE', KEYS[1], ARGV[2])
        return 1
    end
    return 0
    """

    _RELEASE_LEASE_LUA = """
    local value = redis.call('GET', KEYS[1])
    if not value then return 0 end
    local owner, token = string.match(value, '^(.*)|(%d+)$')
    if token then
        if owner == ARGV[1] then return redis.call('DEL', KEYS[1]) end
        return 0
    end
    if value == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
    """

    _ACQUIRE_FENCED_LEASE_LUA = """
    local value = redis.call('GET', KEYS[1])
    if value then
        local owner, token = string.match(value, '^(.*)|(%d+)$')
        if owner ~= ARGV[1] then return 0 end
        redis.call('PEXPIRE', KEYS[1], ARGV[2])
        return tonumber(token)
    end
    local token = redis.call('INCR', KEYS[2])
    redis.call('SET', KEYS[1], ARGV[1] .. '|' .. token, 'PX', ARGV[2])
    return token
    """

    _RENEW_FENCED_LEASE_LUA = """
    if redis.call('GET', KEYS[1]) == ARGV[1] .. '|' .. ARGV[2] then
        redis.call('PEXPIRE', KEYS[1], ARGV[3])
        return 1
    end
    return 0
    """

    _RENEW_MIGRATION_LOCK_LUA = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        redis.call('PEXPIRE', KEYS[1], ARGV[2])
        return 1
    end
    return 0
    """

    _PUBLISH_MIGRATION_LUA = """
    if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
    if redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
    if redis.call('EXISTS', KEYS[2]) == 0 then return -2 end
    redis.call('RENAME', KEYS[2], KEYS[3])
    redis.call('PERSIST', KEYS[3])
    redis.call('SET', KEYS[4], '1')
    return 1
    """

    _RUN_STATE_LUA = """
    return {redis.call('EXISTS', KEYS[1]), redis.call('EXISTS', KEYS[2])}
    """

    _MARK_EXISTING_MIGRATION_LUA = """
    if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
    if redis.call('EXISTS', KEYS[2]) == 0 then return 0 end
    redis.call('SET', KEYS[3], '1')
    return 1
    """

    _RELEASE_MIGRATION_LOCK_LUA = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
    """

    _RELEASE_FENCED_LEASE_LUA = """
    if redis.call('GET', KEYS[1]) == ARGV[1] .. '|' .. ARGV[2] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
    """

    _APPEND_FENCED_LUA = """
    if redis.call('GET', KEYS[3]) ~= ARGV[4] .. '|' .. ARGV[5]
       or redis.call('PTTL', KEYS[3]) <= 0 then
        return -2
    end
    local current_len = redis.call('LLEN', KEYS[1])
    if current_len + 1 ~= tonumber(ARGV[1]) then return -1 end
    redis.call('RPUSH', KEYS[1], ARGV[2])
    redis.call('SET', KEYS[2], '1')
    return tonumber(ARGV[1])
    """

    def __init__(self, redis_client: Any, namespace: str = "continuity") -> None:
        if not re.fullmatch(r"[A-Za-z0-9:_-]+", namespace):
            raise ValueError("namespace may contain only letters, numbers, colon, underscore, and hyphen")
        if not callable(getattr(redis_client, "scan_iter", None)) and not callable(
            getattr(redis_client, "scan", None)
        ):
            raise TypeError("Redis client must support scan_iter or scan to provide complete run listings")
        self._client = redis_client
        self._ns = namespace

    @staticmethod
    def _run_tag(run_id: str) -> str:
        return base64.urlsafe_b64encode(run_id.encode("utf-8")).decode("ascii").rstrip("=") or "empty"

    @staticmethod
    def _step_tag(step_id: str) -> str:
        return base64.urlsafe_b64encode(step_id.encode("utf-8")).decode("ascii").rstrip("=") or "empty"

    def _events_key(self, run_id: str) -> str:
        return f"{self._ns}:{{{self._run_tag(run_id)}}}:events"

    def _lease_key(self, run_id: str, step_id: str) -> str:
        return f"{self._ns}:{{{self._run_tag(run_id)}}}:lease:{self._step_tag(step_id)}"

    def _fencing_counter_key(self, run_id: str, step_id: str) -> str:
        return f"{self._ns}:{{{self._run_tag(run_id)}}}:fence:{self._step_tag(step_id)}"

    def _run_marker_key(self, run_id: str) -> str:
        return f"{self._ns}:{{{self._run_tag(run_id)}}}:run"

    def _legacy_events_key(self, run_id: str) -> str:
        return f"{self._ns}:events:{run_id}"

    def _new_run_state(self, marker_key: str, events_key: str) -> tuple[bool, bool]:
        marker_exists, events_exist = self._client.eval(
            self._RUN_STATE_LUA,
            2,
            marker_key,
            events_key,
        )
        return bool(marker_exists), bool(events_exist)

    def _migrate_legacy_events(self, run_id: str) -> None:
        """Move v0.8 standalone keys to the run hash slot; stop old writers during upgrade."""
        old_key = self._legacy_events_key(run_id)
        new_key = self._events_key(run_id)
        marker_key = self._run_marker_key(run_id)
        lock_key = f"{self._ns}:{{{self._run_tag(run_id)}}}:migration-lock"
        deadline = time.monotonic() + 60.0
        retry_delay = 0.01
        lock_ttl_ms = 30000

        while True:
            marker_exists, new_exists = self._new_run_state(marker_key, new_key)
            old_exists = bool(self._client.exists(old_key))
            if marker_exists:
                if not new_exists:
                    raise RuntimeError(f"Redis run marker exists without event list for run {run_id!r}")
                if old_exists:
                    self._client.delete(old_key)
                return
            if not old_exists:
                if new_exists:
                    marker_exists, new_exists = self._new_run_state(marker_key, new_key)
                    if marker_exists and new_exists:
                        return
                    if not marker_exists and not new_exists:
                        return
                    raise RuntimeError(
                        f"Redis event list for run {run_id!r} has no run marker or legacy source; manual recovery is required"
                    )
                return

            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for legacy Redis migration of run {run_id!r}")

            lock_value = uuid.uuid4().hex
            if not self._client.set(lock_key, lock_value, nx=True, px=lock_ttl_ms):
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Timed out waiting for legacy Redis migration of run {run_id!r}")
                time.sleep(random.uniform(retry_delay, retry_delay * 2))
                retry_delay = min(retry_delay * 2, 0.2)
                continue

            temp_key = f"{self._ns}:{{{self._run_tag(run_id)}}}:migration-temp:{lock_value}"
            try:
                if self._client.exists(marker_key):
                    if self._client.exists(old_key):
                        self._client.delete(old_key)
                    return
                if not self._client.exists(old_key):
                    return

                old_length = int(self._client.llen(old_key))
                if self._client.exists(new_key):
                    old_items = self._client.lrange(old_key, 0, -1)
                    new_items = self._client.lrange(new_key, 0, -1)
                    if old_items != new_items:
                        raise RuntimeError(
                            f"Redis migration state conflicts for run {run_id!r}; preserving both lists for manual recovery"
                        )
                    published = self._client.eval(
                        self._MARK_EXISTING_MIGRATION_LUA,
                        3,
                        lock_key,
                        new_key,
                        marker_key,
                        lock_value,
                    )
                    if int(published) == 1:
                        self._client.delete(old_key)
                        return
                    continue

                serialized_copy = self._client.dump(old_key)
                if serialized_copy is None:
                    continue
                renewed = self._client.eval(
                    self._RENEW_MIGRATION_LOCK_LUA, 1, lock_key, lock_value, lock_ttl_ms
                )
                if int(renewed) != 1:
                    continue
                self._client.restore(temp_key, lock_ttl_ms * 2, serialized_copy)
                if int(self._client.llen(old_key)) != old_length:
                    raise RuntimeError(
                        f"Legacy Redis run {run_id!r} changed during migration; stop v0.8 writers and retry"
                    )
                renewed = self._client.eval(
                    self._RENEW_MIGRATION_LOCK_LUA, 1, lock_key, lock_value, lock_ttl_ms
                )
                if int(renewed) != 1:
                    continue
                published = self._client.eval(
                    self._PUBLISH_MIGRATION_LUA,
                    4,
                    lock_key,
                    temp_key,
                    new_key,
                    marker_key,
                    lock_value,
                )
                if int(published) == 1:
                    self._client.delete(old_key)
                    return
                if int(published) == -2:
                    raise RuntimeError(f"Redis migration copy disappeared for run {run_id!r}")
            finally:
                self._client.delete(temp_key)
                self._client.eval(self._RELEASE_MIGRATION_LOCK_LUA, 1, lock_key, lock_value)

            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for legacy Redis migration of run {run_id!r}")

    def _runs_key(self) -> str:
        return f"{self._ns}:runs"

    def append_event(self, event: CausalEvent, lease: StepLease | None = None) -> CausalEvent:
        self._migrate_legacy_events(event.run_id)
        serialized = json.dumps(event.to_dict(), default=safe_json_default)
        if lease is not None:
            if lease.run_id != event.run_id or lease.step_id != event.step_id:
                raise FencingTokenError("Lease does not authorize this event")
            res = self._client.eval(
                self._APPEND_FENCED_LUA,
                3,
                self._events_key(event.run_id),
                self._run_marker_key(event.run_id),
                self._lease_key(lease.run_id, lease.step_id),
                event.seq,
                serialized,
                event.run_id,
                lease.owner_id,
                lease.fencing_token,
            )
            if int(res) == -2:
                raise FencingTokenError("Lease owner or fencing token is stale or expired")
            if int(res) == -1:
                raise OptimisticConcurrencyError(f"Redis sequence conflict for run {event.run_id} seq {event.seq}")
            return event
        res = self._client.eval(
            self._APPEND_LUA,
            2,
            self._events_key(event.run_id),
            self._run_marker_key(event.run_id),
            event.seq,
            serialized,
            event.run_id,
        )
        if int(res) == -1:
            raise OptimisticConcurrencyError(f"Redis sequence conflict for run {event.run_id} seq {event.seq}")
        return event

    def get_events(self, run_id: str) -> List[CausalEvent]:
        self._migrate_legacy_events(run_id)
        raw_items = self._client.lrange(self._events_key(run_id), 0, -1)
        events: List[CausalEvent] = []
        for item in raw_items:
            text = item.decode("utf-8") if isinstance(item, bytes) else item
            events.append(CausalEvent.from_dict(json.loads(text)))
        return events

    def get_latest_seq(self, run_id: str) -> int:
        self._migrate_legacy_events(run_id)
        return int(self._client.llen(self._events_key(run_id)))

    def acquire_step_lease(self, run_id: str, step_id: str, worker_id: str, ttl_seconds: float = 30.0) -> bool:
        return self.acquire_fenced_step_lease(run_id, step_id, worker_id, ttl_seconds) is not None

    def acquire_fenced_step_lease(
        self, run_id: str, step_id: str, worker_id: str, ttl_seconds: float = 30.0
    ) -> StepLease | None:
        validate_lease_ttl(ttl_seconds)
        self._migrate_legacy_events(run_id)
        ttl_ms = max(1, int(ttl_seconds * 1000))
        token = int(
            self._client.eval(
                self._ACQUIRE_FENCED_LEASE_LUA,
                2,
                self._lease_key(run_id, step_id),
                self._fencing_counter_key(run_id, step_id),
                worker_id,
                ttl_ms,
            )
        )
        if token <= 0:
            return None
        return StepLease(run_id, step_id, worker_id, token)

    def renew_fenced_step_lease(self, lease: StepLease, ttl_seconds: float = 30.0) -> StepLease | None:
        validate_lease_ttl(ttl_seconds)
        renewed = self._client.eval(
            self._RENEW_FENCED_LEASE_LUA,
            1,
            self._lease_key(lease.run_id, lease.step_id),
            lease.owner_id,
            lease.fencing_token,
            max(1, int(ttl_seconds * 1000)),
        )
        return lease if int(renewed) == 1 else None

    def release_fenced_step_lease(self, lease: StepLease) -> None:
        self._client.eval(
            self._RELEASE_FENCED_LEASE_LUA,
            1,
            self._lease_key(lease.run_id, lease.step_id),
            lease.owner_id,
            lease.fencing_token,
        )

    def release_step_lease(self, run_id: str, step_id: str, worker_id: str) -> None:
        key = self._lease_key(run_id, step_id)
        self._client.eval(self._RELEASE_LEASE_LUA, 1, key, worker_id)

    def list_runs(self) -> List[str]:
        prefix = f"{self._ns}:{{"
        runs = set()

        def add_marker_key(raw_key: Any) -> None:
            key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else raw_key
            if key.startswith(prefix) and key.endswith("}:run"):
                tag = key[len(prefix):-len("}:run")]
                if tag == "empty":
                    runs.add("")
                    return
                padded = tag + "=" * ((4 - len(tag) % 4) % 4)
                try:
                    runs.add(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
                except Exception:
                    return

        scan_iter = getattr(self._client, "scan_iter", None)
        if callable(scan_iter):
            for raw_key in scan_iter(match=f"{self._ns}:*:run"):
                add_marker_key(raw_key)
        else:
            cursor = 0
            while True:
                cursor, raw_keys = self._client.scan(cursor=cursor, match=f"{self._ns}:*:run", count=1000)
                for raw_key in raw_keys:
                    add_marker_key(raw_key)
                if int(cursor) == 0:
                    break

        legacy_members = self._client.smembers(self._runs_key())
        runs.update(m.decode("utf-8") if isinstance(m, bytes) else m for m in legacy_members)
        return sorted(runs)
