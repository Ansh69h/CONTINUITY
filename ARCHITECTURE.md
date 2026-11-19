# Architecture

CONTINUITY is a Python library for durable execution of individual workflow steps. Its core is dependency-free and records a run's progress as ordered `CausalEvent` records. It is not a workflow scheduler, queue, agent framework, distributed transaction coordinator, or replacement for a framework's checkpoint store.

## Components

- `ContinuityRuntime` and its async counterpart coordinate a step, consult recovery history, acquire a lease, execute a callable, and persist lifecycle events.
- `RecoveryPlanner` and `AsyncRecoveryPlanner` interpret the event history and the step's declared `SideEffectClass` to choose reuse, execution, reconciliation, compensation, or escalation.
- `CausalEvent` records the run, sequence, step, event kind, idempotency key, payload, worker, and timestamp.
- `CausalStoreProtocol` is the storage boundary. The repository supplies in-memory, SQLite, PostgreSQL, and Redis implementations.
- `HttpBridge` and `AsyncHttpBridge` wrap HTTP calls and propagate idempotency and Continuity correlation headers.
- `InvariantChecker` checks seven event-history safety invariants. `run_reliability_campaign` is a randomized model-based campaign, not a proof of correctness.
- `telemetry.py` builds OTLP JSON from stored events and can POST it to an OTLP HTTP endpoint.

## Step lifecycle

The runtime identifies a step by its run and step IDs and derives an idempotency key from the step inputs. If the history contains a compatible commit, it reuses the stored output. Otherwise it asks the planner what is safe for the step's effect class. When executing, it records intent and start events, invokes the callable, and records either failure or commit. An immutable `StepLease` carries a monotonically increasing fencing token. Runtime appends validate the owner, token, and lease freshness atomically with event sequencing. A configured operator authorizer can record a durable approval for one additional planner-mediated attempt; the attempt limit still applies.

The four effect classes are `PURE`, `IDEMPOTENT`, `COMPENSATABLE`, and `IRREVERSIBLE`. Their names express application-level recovery expectations; they do not make an external service transactional. A remote operation still needs its own idempotency or reconciliation contract.

## Storage and consistency

SQLite uses WAL, `synchronous=FULL`, and a busy timeout. It is the simplest local option and is intended for a local filesystem; do not assume network filesystems provide SQLite's expected locking and durability behavior. PostgreSQL and Redis implementations are optional and have no mandatory client dependency.

The production-readiness branch adds a fenced append contract. In-memory and SQLite implementations validate inside a lock/`BEGIN IMMEDIATE` transaction. PostgreSQL locks the lease row and uses database time; Redis validates and appends in one Lua script. SQLite is appropriate for a single host. PostgreSQL and Redis implementations have service-backed CI tests added, but those must pass in GitHub CI before claiming live-service verification. These checks protect the event log; a third-party effect still needs downstream idempotency or enforcement of the fencing token.

New events carry schema version 1. Dictionaries and table rows without the field are read as the v0.8 legacy shape; unsupported explicit versions are rejected. SQLite and PostgreSQL migrate their event and lease tables during schema setup. Redis reads/migrates the previous standalone event-list key to a run-slot key; stop older writers during upgrade. Back up persisted data before upgrading.

## Trust boundaries

Application callables, external APIs, databases, telemetry collectors, and recovery operators are separate trust boundaries. The library does not provide tenant isolation, secret encryption, or application authentication. Event payloads include execution arguments and outputs; store only data you may persist and restrict access to the database. The HTTP bridge supports an optional exact-host allowlist and has a 1 MiB response cap, but does not prevent DNS rebinding. OTLP spans omit payloads and raw run/step identifiers by default; deterministic trace/span IDs still correlate repeated exports of the same run. Callers can opt into run IDs, step IDs, and step names. Idempotency keys are never exported.

## Deployment guidance

Use the in-memory store only for ephemeral processes and tests. Use SQLite for a single-host local workflow on a supported local filesystem. PostgreSQL and Redis service-backed tests are configured in CI. Validate backend topology, failover, persistence, backups, concurrency, and external-effect behavior in your own environment; CI coverage is not a service certification.

See [PRODUCTION_GUIDE.md](PRODUCTION_GUIDE.md), [RECOVERY_GUIDE.md](RECOVERY_GUIDE.md), and [SECURITY.md](SECURITY.md) for operational details and limitations.
