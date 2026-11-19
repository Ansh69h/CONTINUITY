# Changelog

## Unreleased

- Add schema-versioned causal events with compatible SQLite/PostgreSQL migrations and explicit rejection of unsupported schema versions.
- Add monotonic fencing-token lease acquire, renew, and release APIs across in-memory, SQLite, PostgreSQL, and Redis stores; runtime appends validate the current owner/token atomically.
- Add configurable `SideEffectPolicy` requirements and a durable, authorization-hook-gated operator approval event for one planner-mediated recovery attempt.
- Add optional LangGraph, OpenAI Agents SDK local function-tool, and FastAPI adapters under `integrations/`, with independently versioned optional dependencies and examples.
- Add lifecycle-measured OTLP workflow/step spans, sensitive attribute opt-in, structured standard-library logs, and low-cardinality metrics.
- Add the `continuity runs list|inspect|recoveries|verify|resume` CLI; payload inspection is redacted by default.
- Add HTTP bridge exact-host allowlisting, redirect validation, credential-URL rejection, and a 1 MiB default response limit.
- Add live PostgreSQL/Redis integration tests, CI service jobs, benchmark scenarios, and configurable soak tooling; large benchmark/soak profiles remain unrun.
- Add architecture, production, recovery, security, contribution, support, roadmap, implementation-status, and ADR documentation.
- Add `safe_json_default` deterministic serializer supporting datetimes, dataclasses, UUIDs, sets, and custom classes across storage engines and idempotency keys.
- Fix `HttpBridge.request` argument forwarding so HTTP method, URL, and payload are hashed into step idempotency keys.
- Add `py.typed` (PEP 561) type marker for IDE and type checker compatibility.
- Configure `pyproject.toml` pytest discovery to ignore runner script.
- Add regression tests for JSON payload serialization, custom class stability, and HTTP bridge input drift.
- Improve the project README, GitHub repository metadata, and package keywords for accurate discovery.
- Credit Aryan Dev Singh Mankotia as the project inventor and add `CITATION.cff`.
- Halt when a step ID is reused with different inputs or side-effect semantics.
- Require reconciliation or compensation before retrying an uncertain compensatable effect.
- Make PostgreSQL lease acquisition conditional and atomic, and validate table prefixes.
- Make Redis lease renewal and release atomic owner checks.
- Renew active step leases and use unique per-execution lease owners.
- Close HTTP error responses after handling them.
- Add regression tests for the audited recovery and lease cases.
