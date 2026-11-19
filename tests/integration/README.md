# Live store integration tests

These tests require real PostgreSQL and Redis services. The unit suite skips them when the service environment variables are unset.

Start the services from the repository root:

```powershell
docker compose up -d
$env:CONTINUITY_TEST_POSTGRES_DSN = "postgresql://continuity:continuity@localhost:54329/continuity"
$env:CONTINUITY_TEST_REDIS_URL = "redis://localhost:6379/0"
python -m pip install -e ".[dev,postgres,redis]"
python -m pytest -q tests/integration
```

The tests create uniquely prefixed PostgreSQL tables and Redis namespaces, then remove them. Use a dedicated test database and Redis database; do not point these variables at production services.

If Docker is unavailable, start a disposable local PostgreSQL 16 database and Redis 7-compatible service yourself, then set the two environment variables to those local endpoints before running pytest. On Windows, Memurai can provide a Redis-compatible local test service; GitHub Actions run #4 passed against upstream Redis 7.
