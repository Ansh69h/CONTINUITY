from continuity.storage.backends import PostgresCausalStore, RedisCausalStore


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
        owner = b"worker-new"

        def eval(self, script, key_count, key, owner):
            assert key_count == 1
            assert "redis.call('GET', KEYS[1]) == ARGV[1]" in script
            assert "redis.call('DEL', KEYS[1])" in script
            if self.owner == owner.encode("utf-8"):
                self.owner = None
                return 1
            return 0

    client = FakeRedis()
    store = RedisCausalStore(client)
    store.release_step_lease("run", "step", "worker-old")
    assert client.owner == b"worker-new"
