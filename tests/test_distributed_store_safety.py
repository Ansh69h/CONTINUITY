import pytest

from continuity.storage.backends import PostgresCausalStore, RedisCausalStore
from continuity.storage.base import StepLease
from continuity.storage import backends as backend_module


def test_postgres_table_prefix_rejects_sql_fragments():
    try:
        PostgresCausalStore(lambda: None, table_prefix="events; DROP TABLE users")
    except ValueError as exc:
        assert "simple SQL identifier" in str(exc)
    else:
        raise AssertionError("Expected an unsafe PostgreSQL table prefix to be rejected")


def test_postgres_lease_uses_conditional_atomic_upsert():
    class Cursor:
        query = ""

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params):
            self.query = query

        def fetchone(self):
            return None

    class Connection:
        def __init__(self):
            self.cursor_value = Cursor()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return self.cursor_value

        def commit(self):
            pass

    connections = []

    def connect():
        conn = Connection()
        connections.append(conn)
        return conn

    store = PostgresCausalStore(connect)
    acquired = store.acquire_step_lease("run", "step", "worker")
    query = connections[0].cursor_value.query
    assert acquired is False
    assert "ON CONFLICT" in query
    assert "WHERE" in query
    assert "RETURNING worker_id" in query


def test_redis_lease_release_keeps_a_new_owner_lease():
    class FakeRedis:
        value = b"worker-new|1"

        def scan_iter(self, **_kwargs):
            return iter(())

        def eval(self, script, key_count, key, owner):
            assert key_count == 1
            assert "local owner, token" in script
            assert "redis.call('DEL', KEYS[1])" in script
            if self.value is None:
                return 0
            current = self.value.decode("utf-8")
            requested_owner = owner
            parsed_owner, separator, token = current.rpartition("|")
            if separator and token.isdigit():
                if parsed_owner == requested_owner:
                    self.value = None
                    return 1
                return 0
            if current == requested_owner:
                self.value = None
                return 1
            return 0

    client = FakeRedis()
    store = RedisCausalStore(client)
    store.release_step_lease("run", "step", "worker-old")
    assert client.value == b"worker-new|1"


@pytest.mark.parametrize("ttl_seconds", [float("inf"), float("-inf"), float("nan")])
@pytest.mark.parametrize("operation", ["acquire", "renew"])
def test_postgres_rejects_non_finite_lease_ttls_before_connecting(ttl_seconds, operation):
    def connect():
        raise AssertionError("invalid TTL must be rejected before connecting")

    store = PostgresCausalStore(connect)
    with pytest.raises(ValueError, match="finite and positive"):
        if operation == "acquire":
            store.acquire_fenced_step_lease("run", "step", "worker", ttl_seconds=ttl_seconds)
        else:
            store.renew_fenced_step_lease(StepLease("run", "step", "worker", 1), ttl_seconds=ttl_seconds)


@pytest.mark.parametrize("ttl_seconds", [float("inf"), float("-inf"), float("nan")])
@pytest.mark.parametrize("operation", ["acquire", "renew"])
def test_redis_rejects_non_finite_lease_ttls_before_storage_access(ttl_seconds, operation):
    class UnavailableRedis:
        def scan_iter(self, **_kwargs):
            return iter(())

        def __getattr__(self, name):
            raise AssertionError(f"invalid TTL must be rejected before Redis {name}")

    store = RedisCausalStore(UnavailableRedis())
    with pytest.raises(ValueError, match="finite and positive"):
        if operation == "acquire":
            store.acquire_fenced_step_lease("run", "step", "worker", ttl_seconds=ttl_seconds)
        else:
            store.renew_fenced_step_lease(StepLease("run", "step", "worker", 1), ttl_seconds=ttl_seconds)


def test_redis_migration_deadline_is_checked_after_lost_lock(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(backend_module.time, "monotonic", lambda: now[0])

    class FakeRedis:
        set_calls = 0

        def scan_iter(self, **_kwargs):
            return iter(())

        def eval(self, script, *_args):
            if script == RedisCausalStore._RUN_STATE_LUA:
                return 0, 0
            if script == RedisCausalStore._RENEW_MIGRATION_LOCK_LUA:
                now[0] = 61.0
                return 0
            return 1

        def exists(self, key):
            return int(key.endswith(":events:legacy"))

        def set(self, *_args, **_kwargs):
            self.set_calls += 1
            if self.set_calls > 1:
                raise AssertionError("migration retried after its deadline")
            return True

        def llen(self, _key):
            return 1

        def dump(self, _key):
            return b"legacy-copy"

        def delete(self, *_keys):
            return 1

    store = RedisCausalStore(FakeRedis(), namespace="deadline-test")
    with pytest.raises(TimeoutError, match="Timed out waiting for legacy Redis migration"):
        store._migrate_legacy_events("legacy")
