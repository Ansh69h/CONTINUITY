# CONTINUITY Production Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan task by task. Each task ends with a checkable result.

**Goal:** Make the v0.8 runtime safer under lease expiry, add independently verifiable reliability and integration surfaces, and document the verified limits of the project.

**Architecture:** Keep `continuity` dependency-light. Extend its storage contract with durable fencing tokens and versioned event records, then build optional integrations and operational tooling around those public contracts. Preserve existing `SideEffectClass` values and callback APIs where safe; document any intentional protocol migration.

**Tech Stack:** Python 3.10+, SQLite WAL, optional psycopg and redis clients, pytest/unittest, Docker-backed CI services, standard-library CLI and OTLP HTTP exporter.

**Spec:** User-supplied six-agent CONTINUITY engineering brief. This plan converts its acceptance criteria into staged work; this branch must not be merged without user authorization.

## Global Constraints

- Keep mandatory core dependencies empty and optional integrations isolated.
- Preserve synchronous/asynchronous execution, decorators, HTTP bridges, storage backends, and existing imports wherever safe.
- Do not accept stale fencing tokens or blindly retry uncertain irreversible effects.
- Test adapters against their real framework/service versions; mocks remain unit tests only.
- Distinguish implemented, unit-tested, live-integration-tested, load-tested, and soak-tested work.
- Never claim a load/soak/security assessment passed unless it actually ran.
- Work on `engineering/production-readiness`; push a draft PR only after review; never merge or force-push.

## Review Focus

1. A lease expires after a worker's last local check but before its event append; the append must be rejected atomically.
2. A released lease must not reset its fencing sequence; a reacquisition must use a greater token.
3. Existing persisted v0.8 events lack an explicit schema version; reads must remain compatible, and unknown future versions must fail clearly.
4. Cancellation, store errors, and clock movement must not silently turn a failed lease check into a successful commit.
5. Event payloads and telemetry attributes may contain secrets or identifiers; defaults and guidance must not expose more data than necessary.

## Task 1: Version the event schema and extend store migrations

**Files:**
- Modify: `continuity/models.py`
- Modify: `continuity/storage/base.py`
- Modify: `continuity/storage/backends.py`
- Test: `tests/test_event_schema.py`

**Interfaces:**
- `CausalEvent.schema_version: int` defaults to the current schema version.
- `CausalEvent.from_dict()` treats missing version data as the v0.8 legacy version and rejects unsupported future versions with a specific exception.
- SQLite/PostgreSQL schema setup migrates existing event and lease tables transactionally.

**Steps:**
- [x] Add compatibility tests for old dictionaries, current dictionaries, and unknown future versions.
- [x] Add SQLite migration tests using a database created with the v0.8 schema.
- [x] Add PostgreSQL migration query tests and a live-service migration test in the integration suite.
- [x] Implement the schema field/migration support and run focused tests.

## Task 2: Add fencing-token leases and atomic fenced appends

**Files:**
- Modify: `continuity/storage/base.py`
- Modify: `continuity/storage/backends.py`
- Modify: `continuity/runtime.py`
- Modify: `continuity/models.py`
- Test: `tests/test_fencing_tokens.py`

**Interfaces:**
- Add an immutable `StepLease` value containing run ID, step ID, owner ID, and monotonically increasing fencing token.
- Add fenced acquire/renew/release store methods while retaining legacy boolean methods for callers where possible.
- Add a fenced append path that validates lease owner, token, and freshness in the same transaction/script as sequence validation.
- Runtime sync/async paths must pass the token to every event append and abort if renewal or append rejects it.

**Steps:**
- [x] Write a stale-worker regression: A acquires, pauses, expires; B acquires a larger token; A resumes and its append is rejected.
- [x] Test token stability across renewal and monotonic growth across expiry/release for InMemory and SQLite stores.
- [x] Add live SQL/Lua service tests for PostgreSQL and Redis-compatible storage, including stale token rejection and atomic append behavior.
- [x] Implement the fenced storage contract and update runtime lease renewal/append calls.
- [x] Run focused tests, SQLite process-kill/contention checks, async tests, and the 10,000-trial randomized campaign.
- [x] Document the migration path for custom third-party stores and the limits of external-system fencing.

## Task 3: Verify distributed stores against live services

**Files:**
- Create: `tests/integration/test_postgres_store.py`
- Create: `tests/integration/test_redis_store.py`
- Create: `docker-compose.yml` or CI service definitions
- Modify: `.github/workflows/ci.yml`
- Modify: `pyproject.toml`

**Steps:**
- [x] Add service-backed tests for lease takeover, stale-token rejection, event append atomicity, schema migration, and selected migration/expiry failure windows.
- [ ] Add network restart/reconnection tests against upstream Redis and representative PostgreSQL failover behavior.
- [ ] Keep fake-cursor and fake-Redis tests as fast unit checks.
- [x] Add PostgreSQL and upstream Redis jobs with optional extras to CI.
- [x] Run live PostgreSQL and Redis-compatible service tests locally without Docker. Memurai was used for Redis API compatibility; the upstream Redis CI job remains unverified until the first workflow run.

## Task 4: Add configurable side-effect policies and auditable operator recovery

**Files:**
- Create: `continuity/policy.py`
- Modify: `continuity/models.py`
- Modify: `continuity/runtime.py`
- Modify: `continuity/planner.py`
- Test: `tests/test_recovery_policy.py`
- Test: `tests/test_operator_recovery.py`

**Interfaces:**
- Add `SideEffectPolicy` as an optional higher-level policy that maps to the existing four `SideEffectClass` values.
- Preserve current `side_effect`, `reconciler`, and `compensator` arguments.
- Operator decisions must be immutable events and all resume actions must pass through the planner and a fenced lease.

**Steps:**
- [x] Test policy mapping and reject any policy that permits blind retries for irreversible effects.
- [x] Add inspect/approve/resume methods with authorization hooks and durable decision events.
- [x] Test that an approval cannot override changed inputs and that expired/stale leases are rejected.

## Task 5: Define and implement optional framework adapters

**Files:**
- Create: `integrations/protocol/`
- Create: `integrations/langgraph/`
- Create: `integrations/openai-agents/`
- Create: `integrations/fastapi/`
- Create: `tests/integrations/`

**Steps:**
- [x] Define a public generic adapter protocol using only public CONTINUITY APIs.
- [x] Verify official supported framework APIs and version ranges before writing each adapter.
- [x] Add separate optional dependency metadata, runnable examples, tests, and compatibility policy per adapter.
- [x] Keep LangGraph's checkpoint responsibility separate from CONTINUITY's step durability.
- [x] Run each adapter's tests against the declared supported dependency versions.

## Task 6: Implement useful observability and a functional CLI

**Files:**
- Modify: `continuity/telemetry.py`
- Modify: `continuity/runtime.py`
- Create: `continuity/cli.py`
- Modify: `pyproject.toml`
- Test: `tests/test_telemetry.py`
- Test: `tests/test_cli.py`

**Steps:**
- [x] Build actual workflow/step spans from lifecycle events with measured duration and correct parentage.
- [x] Make sensitive attributes configurable and omit raw payloads and idempotency keys from telemetry.
- [x] Add structured logging and low-cardinality metrics without mandatory runtime dependencies.
- [x] Implement `continuity runs list|inspect|recoveries|verify|resume` with validation and safe authorization hooks.
- [x] Test CLI behavior with temporary SQLite data and exporter payloads.

## Task 7: Add security, production, recovery, and open-source documentation

**Files:**
- Create: `ARCHITECTURE.md`, `ROADMAP.md`, `IMPLEMENTATION_STATUS.md`
- Create: `docs/adr/`
- Create: `PRODUCTION_GUIDE.md`, `RECOVERY_GUIDE.md`, `BENCHMARKS.md`
- Create: `SECURITY.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SUPPORT.md`
- Create: `.github/ISSUE_TEMPLATE/`, `.github/pull_request_template.md`

**Steps:**
- [x] Record architecture, interfaces, tradeoffs, and event/lease schema decisions in ADRs.
- [x] Document supported topologies, durability assumptions, host-owned auth/tenant boundaries, secret handling, recovery guarantees, and known limitations.
- [x] Add runnable examples and version compatibility policies for each completed integration.
- [x] Add GitHub issue and pull request templates and tie the roadmap/status to measured evidence.

## Task 8: Add repeatable benchmarks, chaos gates, and soak tooling

**Files:**
- Create: `benchmarks/`
- Create: `tests/soak/`
- Modify: `.github/workflows/ci.yml`
- Modify: `BENCHMARKS.md`

**Steps:**
- [x] Add repeatable 100/500/1,000-worker and 10k/100k-run benchmark scenarios with latency, throughput, memory, CPU, storage, contention, and recovery outputs.
- [x] Add configurable 1/6/24-hour soak scripts and resource monitoring.
- [x] Run a local 1,000-run benchmark and short SQLite soak; label large scenarios and formal-duration soaks as unrun.
- [x] Keep mandatory release gates limited to deterministic CI tests; do not fabricate benchmark or soak results.

## Task 9: Integrated review and draft delivery

**Steps:**
- [x] Run the final full local suite: 105 passed in 74.44 seconds with PostgreSQL 16.16 and Memurai 4.1.7; both wheels build, and compile/diff checks pass.
- [x] Run three earlier CodeRabbit review cycles (15, 12, and 16 issues) and fix verified issues with regression coverage.
- [x] Run a final CodeRabbit review on the complete changed diff: 18 issues; eight were fixed in this branch and ten are documented for follow-up, including one remaining major item in the FastAPI inspection endpoints.
- [ ] Inspect the final complete diff and verify local/remote branch head equality.
- [ ] Push `engineering/production-readiness` and update the draft PR with the review status. Do not merge.
- [x] Check CodeMouse availability; no repository installations are connected, so do not grant new access.
- [x] Record unsupported upstream Redis/failover, network chaos, long soak, and high-scale benchmark gates honestly.
