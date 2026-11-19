# Roadmap

This roadmap describes engineering direction, not a delivery promise. The status of each item must be updated only after its tests and evidence exist. CONTINUITY is currently an early-stage v0.8 library; the six-agent transformation brief's maturity scores are aspirational and have not been independently measured.

## Reliability and persistence

- **Implemented on this branch:** Add an explicit event schema version and transaction-safe SQLite/PostgreSQL migrations while retaining reads of v0.8 records.
- **Implemented on this branch:** Add durable monotonically increasing fencing tokens and atomically validate a token during event append in each store.
- **Implemented and verified:** Add PostgreSQL and Redis-compatible service tests for takeover, stale append rejection, migration, and sequence order. GitHub Actions run #4 passed the PostgreSQL 16 and upstream Redis 7 service job.
- **Planned:** Add deterministic tests for clock movement, network partitions, and failure windows not modeled by the current randomized campaign.

## Integrations and operations

- **Implemented on this branch:** Add a generic adapter protocol plus optional LangGraph, OpenAI Agents SDK local function-tool, and FastAPI packages. They are experimental and have only the version-specific tests documented in `integrations/README.md`.
- **Implemented on this branch:** Add a run inspection/verification CLI and planner-mediated `resume` command with an application authorization hook for operator approval.
- **Implemented on this branch:** Add lifecycle spans, structured logs, and low-cardinality metrics; telemetry omits payloads and sensitive attributes by default.

## Community and verification

- **Implemented on this branch:** Publish architecture, production, recovery, security, contribution, support, roadmap, implementation-status, and ADR guidance.
- **Implemented on this branch:** Add CI jobs for Python 3.10–3.14, PostgreSQL 16/Redis 7 service tests, and optional framework adapters. Package build, static checks, security scans, and a Windows CI job remain planned.
- **Implemented and smoke-tested:** Add repeatable benchmarks and configurable soak tooling. The large 100/500/1,000-worker benchmark matrix and 1/6/24-hour soak profiles remain unrun.

## Release gates

1. Preserve compatibility or document the migration for storage implementers.
2. Add focused regression tests before changing runtime behavior.
3. Run the full local suite and package checks.
4. Run live-service tests for every backend whose support is claimed.
5. Publish benchmark and soak results with hardware, versions, parameters, and raw output.
6. Update [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) from evidence, not target scores.
