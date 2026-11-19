"""Live PostgreSQL store tests; set CONTINUITY_TEST_POSTGRES_DSN to enable."""

import os
import time
import unittest
import uuid
from threading import Thread

import pytest

from continuity import CausalEvent, EventType, FencingTokenError, SideEffectClass
from continuity.storage.backends import PostgresCausalStore


POSTGRES_DSN = os.environ.get("CONTINUITY_TEST_POSTGRES_DSN")


def _new_store():
    import psycopg

    prefix = f"continuity_it_{uuid.uuid4().hex[:16]}"
    store = PostgresCausalStore(lambda: psycopg.connect(POSTGRES_DSN), table_prefix=prefix)
    return store, prefix


def _drop_tables(prefix):
    import psycopg

    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {prefix}_causal_events CASCADE")
            cur.execute(f"DROP TABLE IF EXISTS {prefix}_step_leases CASCADE")


def _event(run_id, seq, event_id, step_id):
    return CausalEvent(
        run_id=run_id,
        seq=seq,
        event_id=event_id,
        step_id=step_id,
        step_name=step_id,
        event_type=EventType.STEP_COMMITTED,
        side_effect=SideEffectClass.PURE,
        idempotency_key="integration-key",
        payload={"sequence": seq},
        worker_id="integration-worker",
    )


@unittest.skipUnless(POSTGRES_DSN, "Set CONTINUITY_TEST_POSTGRES_DSN to run PostgreSQL integration tests")
def test_postgres_fencing_takeover_atomic_append_and_order():
    store, prefix = _new_store()
    try:
        store.ensure_schema()
        store.ensure_schema()  # schema initialization is idempotent
        run_id, step_id = f"pg-run-{uuid.uuid4().hex}", "critical-step"

        stale = store.acquire_fenced_step_lease(run_id, step_id, "worker-a", ttl_seconds=2.0)
        assert stale is not None
        assert store.acquire_fenced_step_lease(run_id, step_id, "worker-b", ttl_seconds=2) is None
        renewed = store.renew_fenced_step_lease(stale, ttl_seconds=2.0)
        assert renewed is not None and renewed.fencing_token == stale.fencing_token
        time.sleep(2.1)

        current = store.acquire_fenced_step_lease(run_id, step_id, "worker-b", ttl_seconds=2)
        assert current is not None and current.fencing_token > stale.fencing_token
        try:
            store.append_event(_event(run_id, 1, "stale-event", step_id), lease=stale)
        except FencingTokenError:
            pass
        else:
            raise AssertionError("Expired PostgreSQL lease was allowed to append")

        store.append_event(_event(run_id, 1, "event-1", step_id), lease=current)
        store.append_event(_event(run_id, 2, "event-2", step_id), lease=current)
        assert [event.seq for event in store.get_events(run_id)] == [1, 2]

        store.release_fenced_step_lease(current)
        next_lease = store.acquire_fenced_step_lease(run_id, step_id, "worker-c", ttl_seconds=2)
        assert next_lease is not None and next_lease.fencing_token > current.fencing_token
    finally:
        _drop_tables(prefix)


@unittest.skipUnless(POSTGRES_DSN, "Set CONTINUITY_TEST_POSTGRES_DSN to run PostgreSQL integration tests")
def test_postgres_migrates_legacy_v08_event_and_lease_tables():
    import psycopg
    from psycopg.types.json import Jsonb

    store, prefix = _new_store()
    try:
        with psycopg.connect(POSTGRES_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""CREATE TABLE {prefix}_causal_events (
                    run_id TEXT NOT NULL, seq BIGINT NOT NULL, event_id TEXT NOT NULL UNIQUE,
                    step_id TEXT NOT NULL, step_name TEXT NOT NULL, event_type TEXT NOT NULL,
                    side_effect TEXT NOT NULL, idempotency_key TEXT NOT NULL,
                    parent_event_ids JSONB NOT NULL, payload JSONB NOT NULL,
                    timestamp DOUBLE PRECISION NOT NULL, worker_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL, PRIMARY KEY (run_id, seq))""")
                cur.execute(f"""CREATE TABLE {prefix}_step_leases (
                    run_id TEXT NOT NULL, step_id TEXT NOT NULL, worker_id TEXT NOT NULL,
                    expires_at DOUBLE PRECISION NOT NULL, PRIMARY KEY (run_id, step_id))""")
                cur.execute(
                    f"""INSERT INTO {prefix}_causal_events (
                        run_id, seq, event_id, step_id, step_name, event_type,
                        side_effect, idempotency_key, parent_event_ids, payload,
                        timestamp, worker_id, attempt
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        "legacy-run", 1, "legacy-event", "legacy-step", "legacy-step",
                        "STEP_INTENT", "PURE", "legacy-key", Jsonb([]), Jsonb({"args": []}),
                        1.0, "legacy-worker", 1,
                    ),
                )

        store.ensure_schema()
        events = store.get_events("legacy-run")
        assert len(events) == 1 and events[0].schema_version == 1
        with psycopg.connect(POSTGRES_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT schema_version FROM {prefix}_causal_events WHERE event_id = %s", ("legacy-event",))
                assert cur.fetchone()[0] == 1
                cur.execute(f"SELECT fencing_token FROM {prefix}_step_leases")
                # The migrated table exposes the token column; no row is required for this migration check.
                assert cur.description[0].name == "fencing_token"
    finally:
        _drop_tables(prefix)


@unittest.skipUnless(POSTGRES_DSN, "Set CONTINUITY_TEST_POSTGRES_DSN to run PostgreSQL integration tests")
def test_postgres_rejects_lease_that_expires_while_waiting_for_row_lock():
    import psycopg

    prefix = f"continuity_it_{uuid.uuid4().hex[:16]}"
    application_name = f"lease_test_{uuid.uuid4().hex[:8]}"
    store = PostgresCausalStore(
        lambda: psycopg.connect(POSTGRES_DSN, application_name=application_name),
        table_prefix=prefix,
    )
    run_id, step_id = f"expiry-run-{uuid.uuid4().hex}", "locked-step"
    try:
        store.ensure_schema()
        lease = store.acquire_fenced_step_lease(run_id, step_id, "worker-a", ttl_seconds=0.8)
        assert lease is not None

        blocker = psycopg.connect(POSTGRES_DSN)
        try:
            with blocker.cursor() as cursor:
                cursor.execute(
                    f"SELECT run_id FROM {prefix}_step_leases WHERE run_id = %s AND step_id = %s FOR UPDATE",
                    (run_id, step_id),
                )
                assert cursor.fetchone() is not None

            append_result = []

            def append_with_expiring_lease():
                try:
                    store.append_event(_event(run_id, 1, "expires-while-locked", step_id), lease=lease)
                except Exception as exc:
                    append_result.append(exc)
                else:
                    append_result.append(None)

            worker = Thread(target=append_with_expiring_lease)
            worker.start()
            deadline = time.monotonic() + 3
            waiting_for_lock = False
            while time.monotonic() < deadline:
                with psycopg.connect(POSTGRES_DSN) as observer:
                    with observer.cursor() as cursor:
                        cursor.execute(
                            "SELECT wait_event_type FROM pg_stat_activity WHERE application_name = %s",
                            (application_name,),
                        )
                        row = cursor.fetchone()
                if row is not None and row[0] == "Lock":
                    waiting_for_lock = True
                    break
                time.sleep(0.02)
            assert waiting_for_lock, "append never entered the lease-row lock wait"

            time.sleep(0.85)
            blocker.commit()
            worker.join(timeout=3)
            assert not worker.is_alive()
            assert len(append_result) == 1
            assert isinstance(append_result[0], FencingTokenError)
        finally:
            blocker.close()
    finally:
        _drop_tables(prefix)


@unittest.skipUnless(POSTGRES_DSN, "Set CONTINUITY_TEST_POSTGRES_DSN to run PostgreSQL integration tests")
def test_postgres_requires_transactions_for_fenced_operations():
    import psycopg

    prefix = f"continuity_it_{uuid.uuid4().hex[:16]}"
    store = PostgresCausalStore(lambda: psycopg.connect(POSTGRES_DSN), table_prefix=prefix)
    autocommit_store = PostgresCausalStore(
        lambda: psycopg.connect(POSTGRES_DSN, autocommit=True),
        table_prefix=prefix,
    )
    run_id, step_id = f"autocommit-run-{uuid.uuid4().hex}", "step"
    try:
        store.ensure_schema()
        lease = store.acquire_fenced_step_lease(run_id, step_id, "worker-a", ttl_seconds=30)
        assert lease is not None

        with pytest.raises(ValueError, match="autocommit=False"):
            autocommit_store.acquire_fenced_step_lease(run_id, "another-step", "worker-b", ttl_seconds=30)
        with pytest.raises(ValueError, match="autocommit=False"):
            autocommit_store.renew_fenced_step_lease(lease, ttl_seconds=30)
        with pytest.raises(ValueError, match="autocommit=False"):
            autocommit_store.append_event(_event(run_id, 1, "autocommit-event", step_id), lease=lease)

        assert store.get_events(run_id) == []
    finally:
        _drop_tables(prefix)


@unittest.skipUnless(POSTGRES_DSN, "Set CONTINUITY_TEST_POSTGRES_DSN to run PostgreSQL integration tests")
def test_postgres_acquisition_ttl_starts_after_conflicting_insert_resolves():
    import psycopg

    prefix = f"continuity_it_{uuid.uuid4().hex[:16]}"
    application_name = f"lease_acquire_{uuid.uuid4().hex[:8]}"
    setup_store = PostgresCausalStore(lambda: psycopg.connect(POSTGRES_DSN), table_prefix=prefix)
    acquiring_store = PostgresCausalStore(
        lambda: psycopg.connect(POSTGRES_DSN, application_name=application_name),
        table_prefix=prefix,
    )
    run_id, step_id = f"delayed-insert-{uuid.uuid4().hex}", "step"
    blocker = None
    worker = None
    result = []
    try:
        setup_store.ensure_schema()
        blocker = psycopg.connect(POSTGRES_DSN)
        with blocker.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO {prefix}_step_leases
                    (run_id, step_id, worker_id, expires_at, fencing_token)
                    VALUES (%s, %s, %s, '-Infinity', 1)""",
                (run_id, step_id, "uncommitted-worker"),
            )

        def acquire():
            try:
                result.append(acquiring_store.acquire_fenced_step_lease(run_id, step_id, "worker-a", 0.25))
            except Exception as exc:
                result.append(exc)

        worker = Thread(target=acquire)
        worker.start()
        deadline = time.monotonic() + 3
        waiting_for_lock = False
        while time.monotonic() < deadline:
            with psycopg.connect(POSTGRES_DSN) as observer:
                with observer.cursor() as cursor:
                    cursor.execute(
                        "SELECT wait_event_type FROM pg_stat_activity WHERE application_name = %s",
                        (application_name,),
                    )
                    row = cursor.fetchone()
            if row is not None and row[0] == "Lock":
                waiting_for_lock = True
                break
            time.sleep(0.02)
        assert waiting_for_lock, "lease acquisition never waited on the conflicting insert"

        time.sleep(0.35)
        blocker.rollback()
        worker.join(timeout=3)
        assert not worker.is_alive()
        assert len(result) == 1 and not isinstance(result[0], Exception)
        lease = result[0]
        assert lease is not None
        setup_store.append_event(_event(run_id, 1, "fresh-after-wait", step_id), lease=lease)
    finally:
        if blocker is not None:
            blocker.close()
        if worker is not None and worker.is_alive():
            if blocker is not None:
                blocker.rollback()
            worker.join(timeout=3)
        _drop_tables(prefix)


@unittest.skipUnless(POSTGRES_DSN, "Set CONTINUITY_TEST_POSTGRES_DSN to run PostgreSQL integration tests")
def test_postgres_renewal_rejects_a_lease_that_expires_while_waiting_for_row_lock():
    import psycopg

    prefix = f"continuity_it_{uuid.uuid4().hex[:16]}"
    application_name = f"lease_renew_{uuid.uuid4().hex[:8]}"
    setup_store = PostgresCausalStore(lambda: psycopg.connect(POSTGRES_DSN), table_prefix=prefix)
    renewing_store = PostgresCausalStore(
        lambda: psycopg.connect(POSTGRES_DSN, application_name=application_name),
        table_prefix=prefix,
    )
    run_id, step_id = f"renew-expiry-{uuid.uuid4().hex}", "step"
    blocker = None
    worker = None
    renewed = []
    try:
        setup_store.ensure_schema()
        lease = setup_store.acquire_fenced_step_lease(run_id, step_id, "worker-a", ttl_seconds=0.8)
        assert lease is not None
        blocker = psycopg.connect(POSTGRES_DSN)
        with blocker.cursor() as cursor:
            cursor.execute(
                f"SELECT run_id FROM {prefix}_step_leases WHERE run_id = %s AND step_id = %s FOR UPDATE",
                (run_id, step_id),
            )
            assert cursor.fetchone() is not None

        worker = Thread(target=lambda: renewed.append(renewing_store.renew_fenced_step_lease(lease, ttl_seconds=2)))
        worker.start()
        deadline = time.monotonic() + 3
        waiting_for_lock = False
        while time.monotonic() < deadline:
            with psycopg.connect(POSTGRES_DSN) as observer:
                with observer.cursor() as cursor:
                    cursor.execute(
                        "SELECT wait_event_type FROM pg_stat_activity WHERE application_name = %s",
                        (application_name,),
                    )
                    row = cursor.fetchone()
            if row is not None and row[0] == "Lock":
                waiting_for_lock = True
                break
            time.sleep(0.02)
        assert waiting_for_lock, "renewal never entered the lease-row lock wait"

        time.sleep(0.85)
        blocker.commit()
        worker.join(timeout=3)
        assert not worker.is_alive()
        assert renewed == [None]
    finally:
        if blocker is not None:
            blocker.close()
        if worker is not None and worker.is_alive():
            if blocker is not None:
                blocker.rollback()
            worker.join(timeout=3)
        _drop_tables(prefix)
