"""Live Redis store tests; set CONTINUITY_TEST_REDIS_URL to enable."""

import json
import os
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Event

from continuity import CausalEvent, EventType, FencingTokenError, SideEffectClass
from continuity.storage.backends import RedisCausalStore


REDIS_URL = os.environ.get("CONTINUITY_TEST_REDIS_URL")


def _new_store():
    import redis

    namespace = f"continuity-it-{uuid.uuid4().hex[:12]}"
    client = redis.Redis.from_url(REDIS_URL, decode_responses=False)
    return RedisCausalStore(client, namespace=namespace), client, namespace


def _cleanup(client, namespace):
    for key in client.scan_iter(match=f"{namespace}:*"):
        client.delete(key)


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


def _legacy_event(run_id):
    return json.dumps(
        {
            "run_id": run_id,
            "seq": 1,
            "event_id": f"legacy-{run_id}",
            "step_id": "legacy-step",
            "step_name": "legacy-step",
            "event_type": "STEP_INTENT",
            "side_effect": "PURE",
            "idempotency_key": "legacy-key",
            "parent_event_ids": [],
            "payload": {"args": []},
            "timestamp": 1.0,
            "worker_id": "legacy-worker",
            "attempt": 1,
        }
    )


@unittest.skipUnless(REDIS_URL, "Set CONTINUITY_TEST_REDIS_URL to run Redis integration tests")
def test_redis_fencing_takeover_atomic_append_and_order():
    store, client, namespace = _new_store()
    try:
        assert client.ping()
        run_id, step_id = f"redis-run-{uuid.uuid4().hex}", "critical-step"
        stale = store.acquire_fenced_step_lease(run_id, step_id, "worker-a", ttl_seconds=0.15)
        assert stale is not None
        assert store.acquire_fenced_step_lease(run_id, step_id, "worker-b", ttl_seconds=2) is None
        time.sleep(0.25)
        current = store.acquire_fenced_step_lease(run_id, step_id, "worker-b", ttl_seconds=2)
        assert current is not None and current.fencing_token > stale.fencing_token

        try:
            store.append_event(_event(run_id, 1, "stale-event", step_id), lease=stale)
        except FencingTokenError:
            pass
        else:
            raise AssertionError("Expired Redis lease was allowed to append")

        store.append_event(_event(run_id, 1, "event-1", step_id), lease=current)
        store.append_event(_event(run_id, 2, "event-2", step_id), lease=current)
        assert [event.seq for event in store.get_events(run_id)] == [1, 2]

        renew_step = "renewal-step"
        renewing = store.acquire_fenced_step_lease(run_id, renew_step, "worker-renew", ttl_seconds=2)
        renewed = store.renew_fenced_step_lease(renewing, ttl_seconds=2)
        assert renewing is not None and renewed is not None
        assert renewed.fencing_token == renewing.fencing_token
        store.release_fenced_step_lease(current)
        next_lease = store.acquire_fenced_step_lease(run_id, step_id, "worker-c", ttl_seconds=2)
        assert next_lease is not None and next_lease.fencing_token > current.fencing_token
    finally:
        _cleanup(client, namespace)
        client.close()


@unittest.skipUnless(REDIS_URL, "Set CONTINUITY_TEST_REDIS_URL to run Redis integration tests")
def test_redis_migrates_legacy_v08_event_list():
    store, client, namespace = _new_store()
    try:
        run_id = f"legacy-run-{uuid.uuid4().hex}"
        old_key = f"{namespace}:events:{run_id}"
        client.rpush(old_key, _legacy_event(run_id))
        source_traversals = []
        original_dump = client.dump
        original_lrange = client.lrange

        def tracked_dump(key):
            if key == old_key:
                source_traversals.append("dump")
            return original_dump(key)

        def tracked_lrange(key, start, end):
            if key == old_key:
                source_traversals.append("lrange")
            return original_lrange(key, start, end)

        client.dump = tracked_dump
        client.lrange = tracked_lrange
        events = store.get_events(run_id)
        assert len(events) == 1 and events[0].schema_version == 1
        assert source_traversals == ["dump"]
        assert client.exists(old_key) == 0
        assert store.get_latest_seq(run_id) == 1
        assert client.pttl(store._events_key(run_id)) == -1
    finally:
        _cleanup(client, namespace)
        client.close()


@unittest.skipUnless(REDIS_URL, "Set CONTINUITY_TEST_REDIS_URL to run Redis integration tests")
def test_redis_recovers_a_copied_legacy_list_missing_its_marker():
    store, client, namespace = _new_store()
    try:
        run_id = f"partial-copy-{uuid.uuid4().hex}"
        serialized = _legacy_event(run_id)
        client.rpush(store._legacy_events_key(run_id), serialized)
        client.rpush(store._events_key(run_id), serialized)

        events = store.get_events(run_id)

        assert len(events) == 1
        assert client.exists(store._run_marker_key(run_id)) == 1
        assert client.exists(store._legacy_events_key(run_id)) == 0
    finally:
        _cleanup(client, namespace)
        client.close()


@unittest.skipUnless(REDIS_URL, "Set CONTINUITY_TEST_REDIS_URL to run Redis integration tests")
def test_redis_reads_wait_for_an_active_legacy_migration_lock():
    store, client, namespace = _new_store()
    try:
        run_id = f"locked-legacy-{uuid.uuid4().hex}"
        client.rpush(store._legacy_events_key(run_id), _legacy_event(run_id))
        lock_key = f"{namespace}:{{{store._run_tag(run_id)}}}:migration-lock"
        client.set(lock_key, "other-migrator", px=30000)

        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(store.get_events, run_id)
            assert not pending.done()
            client.delete(lock_key)
            events = pending.result(timeout=5)

        assert len(events) == 1
        assert events[0].seq == 1
    finally:
        _cleanup(client, namespace)
        client.close()


@unittest.skipUnless(REDIS_URL, "Set CONTINUITY_TEST_REDIS_URL to run Redis integration tests")
def test_redis_first_append_is_not_mistaken_for_an_inconsistent_migration():
    store, client, namespace = _new_store()
    marker_checked = Event()
    continue_reader = Event()
    try:
        run_id = f"first-append-race-{uuid.uuid4().hex}"
        marker_key = store._run_marker_key(run_id)
        original_state = store._new_run_state
        paused = False

        def pause_after_state_check(marker, events):
            nonlocal paused
            result = original_state(marker, events)
            if marker == marker_key and not paused:
                paused = True
                marker_checked.set()
                if not continue_reader.wait(timeout=5):
                    raise TimeoutError("test did not resume the Redis reader")
            return result

        store._new_run_state = pause_after_state_check
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(store.get_events, run_id)
            assert marker_checked.wait(timeout=5)
            store.append_event(_event(run_id, 1, "first-event", "step"))
            continue_reader.set()
            events = pending.result(timeout=5)

        assert [event.event_id for event in events] == ["first-event"]
    finally:
        continue_reader.set()
        _cleanup(client, namespace)
        client.close()


@unittest.skipUnless(REDIS_URL, "Set CONTINUITY_TEST_REDIS_URL to run Redis integration tests")
def test_redis_stale_migrator_cannot_publish_after_lock_takeover():
    store, client, namespace = _new_store()
    paused_after_snapshot = Event()
    resume_stale_migrator = Event()
    try:
        run_id = f"stale-migrator-{uuid.uuid4().hex}"
        client.rpush(store._legacy_events_key(run_id), _legacy_event(run_id))
        lock_key = f"{namespace}:{{{store._run_tag(run_id)}}}:migration-lock"
        original_dump = client.dump
        first_snapshot = True

        def delayed_dump(key):
            nonlocal first_snapshot
            payload = original_dump(key)
            if key == store._legacy_events_key(run_id) and first_snapshot:
                first_snapshot = False
                paused_after_snapshot.set()
                if not resume_stale_migrator.wait(timeout=5):
                    raise TimeoutError("test did not release stale migrator")
            return payload

        client.dump = delayed_dump
        with ThreadPoolExecutor(max_workers=2) as executor:
            stale = executor.submit(store.get_events, run_id)
            assert paused_after_snapshot.wait(timeout=5)
            client.delete(lock_key)  # Simulate the migration lock expiring.
            current = executor.submit(store.get_events, run_id)
            assert len(current.result(timeout=5)) == 1
            resume_stale_migrator.set()
            assert len(stale.result(timeout=5)) == 1

        assert len(store.get_events(run_id)) == 1
        assert client.llen(store._events_key(run_id)) == 1
    finally:
        resume_stale_migrator.set()
        _cleanup(client, namespace)
        client.close()


@unittest.skipUnless(REDIS_URL, "Set CONTINUITY_TEST_REDIS_URL to run Redis integration tests")
def test_redis_run_listing_includes_short_new_ids_and_unmigrated_legacy_runs():
    store, client, namespace = _new_store()
    try:
        store.append_event(_event("r", 1, "short-run-event", "step"))
        store.append_event(_event("", 1, "empty-run-event", "step"))
        legacy_run_id = "not-yet-migrated"
        client.sadd(store._runs_key(), legacy_run_id)
        client.rpush(store._legacy_events_key(legacy_run_id), _legacy_event(legacy_run_id))

        listed = store.list_runs()

        assert "r" in listed
        assert "" in listed
        assert legacy_run_id in listed
    finally:
        _cleanup(client, namespace)
        client.close()


@unittest.skipUnless(REDIS_URL, "Set CONTINUITY_TEST_REDIS_URL to run Redis integration tests")
def test_redis_run_listing_uses_scan_when_scan_iter_is_unavailable():
    client = __import__("redis").Redis.from_url(REDIS_URL, decode_responses=False)
    namespace = f"continuity-it-{uuid.uuid4().hex[:12]}"

    class ScanOnlyRedis:
        def __init__(self, wrapped):
            self._wrapped = wrapped

        def __getattr__(self, name):
            if name == "scan_iter":
                raise AttributeError(name)
            return getattr(self._wrapped, name)

    store = RedisCausalStore(ScanOnlyRedis(client), namespace=namespace)
    try:
        run_id = "new-run-with-scan-only-client"
        store.append_event(_event(run_id, 1, "scan-only-event", "step"))

        assert store.list_runs() == [run_id]
    finally:
        _cleanup(client, namespace)
        client.close()


@unittest.skipUnless(REDIS_URL, "Set CONTINUITY_TEST_REDIS_URL to run Redis integration tests")
def test_legacy_release_cannot_delete_a_fenced_lease_when_owner_contains_delimiter():
    store, client, namespace = _new_store()
    try:
        run_id, step_id = "release-owner", "step"
        lease = store.acquire_fenced_step_lease(run_id, step_id, "alice", ttl_seconds=10)
        assert lease is not None

        store.release_step_lease(run_id, step_id, f"alice|{lease.fencing_token}")

        assert store.renew_fenced_step_lease(lease, ttl_seconds=10) == lease
    finally:
        _cleanup(client, namespace)
        client.close()
