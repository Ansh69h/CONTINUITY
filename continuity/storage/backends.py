from __future__ import annotations
import json
import re
import time
from typing import Any, List
from ..models import CausalEvent
from .base import OptimisticConcurrencyError


class PostgresCausalStore:
    """Optional PostgreSQL distributed causal store (accepts any DB-API 2.0 / psycopg connection factory)."""

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
                        PRIMARY KEY (run_id, seq)
                    );
                """)
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self._leases_table} (
                        run_id TEXT NOT NULL,
                        step_id TEXT NOT NULL,
                        worker_id TEXT NOT NULL,
                        expires_at DOUBLE PRECISION NOT NULL,
                        PRIMARY KEY (run_id, step_id)
                    );
                """)
            conn.commit()

    def append_event(self, event: CausalEvent) -> CausalEvent:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
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
                            timestamp, worker_id, attempt
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                            json.dumps(event.parent_event_ids),
                            json.dumps(event.payload),
                            event.timestamp,
                            event.worker_id,
                            event.attempt,
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
                           timestamp, worker_id, attempt
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
        now = time.time()
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._leases_table} (run_id, step_id, worker_id, expires_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (run_id, step_id) DO UPDATE SET
                        worker_id = EXCLUDED.worker_id,
                        expires_at = EXCLUDED.expires_at
                    WHERE {self._leases_table}.expires_at <= %s
                       OR {self._leases_table}.worker_id = EXCLUDED.worker_id
                    RETURNING worker_id
                    """,
                    (run_id, step_id, worker_id, now + ttl_seconds, now),
                )
                acquired = cur.fetchone() is not None
            conn.commit()
        return acquired

    def release_step_lease(self, run_id: str, step_id: str, worker_id: str) -> None:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self._leases_table} WHERE run_id = %s AND step_id = %s AND worker_id = %s",
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
    local runs_key = KEYS[2]
    local expected_seq = tonumber(ARGV[1])
    local payload = ARGV[2]
    local run_id = ARGV[3]
    local current_len = redis.call('LLEN', list_key)
    if current_len + 1 ~= expected_seq then
        return -1
    end
    redis.call('RPUSH', list_key, payload)
    redis.call('SADD', runs_key, run_id)
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
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
    """

    def __init__(self, redis_client: Any, namespace: str = "continuity") -> None:
        self._client = redis_client
        self._ns = namespace

    def _events_key(self, run_id: str) -> str:
        return f"{self._ns}:events:{run_id}"

    def _lease_key(self, run_id: str, step_id: str) -> str:
        return f"{self._ns}:lease:{run_id}:{step_id}"

    def _runs_key(self) -> str:
        return f"{self._ns}:runs"

    def append_event(self, event: CausalEvent) -> CausalEvent:
        serialized = json.dumps(event.to_dict())
        res = self._client.eval(
            self._APPEND_LUA,
            2,
            self._events_key(event.run_id),
            self._runs_key(),
            event.seq,
            serialized,
            event.run_id,
        )
        if int(res) == -1:
            raise OptimisticConcurrencyError(f"Redis sequence conflict for run {event.run_id} seq {event.seq}")
        return event

    def get_events(self, run_id: str) -> List[CausalEvent]:
        raw_items = self._client.lrange(self._events_key(run_id), 0, -1)
        events: List[CausalEvent] = []
        for item in raw_items:
            text = item.decode("utf-8") if isinstance(item, bytes) else item
            events.append(CausalEvent.from_dict(json.loads(text)))
        return events

    def get_latest_seq(self, run_id: str) -> int:
        return int(self._client.llen(self._events_key(run_id)))

    def acquire_step_lease(self, run_id: str, step_id: str, worker_id: str, ttl_seconds: float = 30.0) -> bool:
        key = self._lease_key(run_id, step_id)
        acquired = self._client.set(key, worker_id, nx=True, px=int(ttl_seconds * 1000))
        if acquired:
            return True
        renewed = self._client.eval(
            self._RENEW_LEASE_LUA,
            1,
            key,
            worker_id,
            int(ttl_seconds * 1000),
        )
        return int(renewed) == 1

    def release_step_lease(self, run_id: str, step_id: str, worker_id: str) -> None:
        key = self._lease_key(run_id, step_id)
        self._client.eval(self._RELEASE_LEASE_LUA, 1, key, worker_id)

    def list_runs(self) -> List[str]:
        members = self._client.smembers(self._runs_key())
        return sorted(m.decode("utf-8") if isinstance(m, bytes) else m for m in members)
