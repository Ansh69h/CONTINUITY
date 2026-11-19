# Implementation status

Status snapshot for the v0.8 codebase while the production-readiness branch is under development. It is not a release certification. “Implemented” below means a code path exists; “verified” is limited to the test evidence named here.

## Implemented on this branch

| Area | Current state | Evidence / limit |
| --- | --- | --- |
| Sync and async steps | Implemented | Runtime, planners, and tests are present. |
| Side-effect-aware recovery | Implemented | Four classes and reconciliation/compensation callbacks exist; correctness still depends on application callbacks and external services. |
| Stores | Implemented | In-memory and SQLite are locally tested. PostgreSQL 16.16 and Memurai 4.1.7 (Redis 7.2.11-compatible) service integrations pass locally; upstream Redis 7 remains pending CI. |
| Event-history checks | Implemented | Seven invariant checks exist. The checker validates recorded histories; it does not prove external exactly-once effects. |
| Randomized campaign | Implemented | Defaults to 10,000 trials. The campaign primarily exercises an in-memory model and samples SQLite; it is not a 10,000-trial live database test or formal proof. |
| SQLite durability checks | Implemented | Existing tests cover process termination and contention scenarios on the local SQLite backend. Results apply to those tests and the host environment. |
| Event schema version | Implemented | New records use schema v1; missing versions are treated as v0.8 legacy data; unknown explicit versions fail clearly. SQLite and PostgreSQL migrations pass local tests. Redis JSON reads are version-aware. |
| Fenced leases | Implemented | Stale-owner/takeover tests pass on in-memory, SQLite, PostgreSQL 16.16, and Memurai 4.1.7. The upstream Redis 7 GitHub service test remains pending. |
| Operator recovery | Implemented | Optional `SideEffectPolicy`, required-callback validation, a configured authorization hook, and a durable `RECOVERY_APPROVED` event support one planner-mediated attempt within `max_attempts`. |
| OTLP JSON export | Implemented | Workflow and step spans use lifecycle timestamps. Payloads and identifiers are omitted by default; run/step identifiers and step names require explicit opt-in. Idempotency keys are never exported. Standard-library structured logs and low-cardinality counters are available. |
| CLI | Implemented | `continuity runs list|inspect|recoveries|verify|resume`; inspection redacts payloads by default. Resume requires a callable and continues through the planner. |
| HTTP bridge | Implemented | Adds idempotency/correlation headers, optional exact-host allowlisting (including redirect targets), and a 1 MiB default response cap. It does not provide application authentication or DNS-rebinding protection. |

## Verification status on this PC

- Full local pytest suite: 105 passed in 74.44 seconds with local PostgreSQL 16.16 and Memurai 4.1.7 enabled. The 17 warnings are a dependency deprecation warning from LangSmith; no tests were skipped.
- PostgreSQL and Redis-compatible integration tests ran in that full suite. Memurai is not the upstream Redis 7 server; GitHub Actions still needs to verify the Redis 7 service job.
- An 8-worker, 1,000-run SQLite microbenchmark completed before the concurrent WAL-initialization fix with 0 failures at 2.63 runs/second; p50/p95/p99 workflow latency was 2.53/6.19/10.29 seconds. Ten recovery samples completed with 0 failures (mean 89.95 ms). This is a local contention measurement, not a capacity guarantee.
- An 8-worker, 10-second SQLite soak smoke completed 198 workflows with 0 failures and 397,579 bytes peak Python-traced memory. This does not count as a 1/6/24-hour soak.
- A 16-thread concurrent SQLite store-initialization regression test exposed and then verified a fix for simultaneous WAL initialization.
- Core and integrations wheels build locally (`continuity_runtime` 0.8.0 and `continuity_integrations` 0.1.0). `compileall`, `git diff --check`, and `git diff --cached --check` pass after the review fixes.

## Final review snapshot

- CodeRabbit's final local review raised 18 issues. Eight have been corrected in this branch and exercised by regression tests. Ten remain for follow-up; one is major: FastAPI's async inspection endpoints still call synchronous store reads on the event loop.
- The remaining minor items cover soak CLI input validation, signal shutdown handling and checkpoint snapshots; timing and cleanup in PostgreSQL tests; synchronization in a Redis migration test; adapter installation instructions; Docker readiness instructions; and fencing-counter restart durability documentation.
- This is a draft readiness review, not a release certification. Upstream Redis 7 CI verification, independent security review, and long-duration soak evidence remain pending.

## Not yet available or not verified

- Independent external security audit, formal verification, tenant isolation, or payload encryption.
- Upstream Redis 7 service execution from this PC; the local Windows-compatible service was Memurai 4.1.7, not the upstream Redis server. PostgreSQL passed locally; GitHub Actions service jobs still await their first run.
- Network partition, Redis restart, lost-ack, clock-skew, long-run soak, and high-concurrency benchmark evidence.
- A measured maturity score; the six-agent brief's target scores are aspirational.

## CI snapshot

GitHub Actions runs pytest on Python 3.10 through 3.14 and has a separate PostgreSQL 16 / Redis 7 integration job. Static type-checking, security scanning, production workload benchmarks, and long soak jobs are not CI gates.
