# CONTINUITY: Python Durable Workflow Recovery for AI Agents

[![CI](https://github.com/Ansh69h/CONTINUITY/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Ansh69h/CONTINUITY/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-orange.svg)](LICENSE)

**CONTINUITY is a Python runtime for durable workflow execution and crash recovery.** It helps AI agents, automation services, and data pipelines resume interrupted work using a causal event log, idempotent retries, external reconciliation, and compensating actions.

Use it when a workflow crosses API calls, tools, databases, or workers and must handle process restarts, timeouts, lost acknowledgements, or concurrent execution safely.

## Features

- **Durable execution log:** SQLite WAL storage records step intent, start, failure, recovery decisions, and commit events.
- **Crash recovery:** resume from the same `run_id`; reuse committed outputs and apply a recovery policy to incomplete steps.
- **Effect-aware retries:** classify work as `PURE`, `IDEMPOTENT`, `COMPENSATABLE`, or `IRREVERSIBLE` to guide retry, reconciliation, compensation, or escalation.
- **Synchronous and asynchronous APIs:** call `ContinuityRuntime.step()` directly or use the `recoverable` and `recoverable_async` decorators.
- **HTTP workflow bridge:** `HttpBridge` adds an idempotency key and can reconcile or compensate an API operation.
- **Storage choices:** in-memory and SQLite stores are built in; PostgreSQL and Redis stores accept caller-provided connections or clients.
- **Reliability validation:** deterministic fault injection, invariant checks, process-kill recovery tests, multi-worker contention tests, and a 10,000-trial randomized simulation.
- **OpenTelemetry export:** build an OTLP trace payload and send it explicitly with `export_otlp_traces()`.

## Quick start

```bash
git clone https://github.com/Ansh69h/CONTINUITY.git
cd CONTINUITY
python -m pip install -e ".[dev]"
```

Persist a small workflow in SQLite and reuse its committed result after a restart:

```python
from continuity import ContinuityRuntime, SQLiteCausalStore, SideEffectClass

store = SQLiteCausalStore("continuity.db")
runtime = ContinuityRuntime(store=store, run_id="invoice-2042")

def calculate_tax(amount: float) -> float:
    return round(amount * 0.08, 2)

tax = runtime.step(
    calculate_tax,
    125.00,
    step_id="calculate-tax",
    side_effect=SideEffectClass.PURE,
)
print(tax)  # 10.0
```

Keep the same `run_id`, `step_id`, and inputs when recovering a run. Use a new `step_id` for a distinct operation. A step ID with changed inputs or a changed side-effect class halts for review rather than returning stale output.

## Choosing a recovery policy

| Side-effect class | Recovery behavior |
| --- | --- |
| `PURE` | Re-execute incomplete work. |
| `IDEMPOTENT` | Retry with the same operation identity; the external service must honor idempotency for its effects. |
| `COMPENSATABLE` | Reconcile or compensate an uncertain attempt before retrying. |
| `IRREVERSIBLE` | Reconcile before retrying; otherwise stop for operator review. |

For HTTP calls, `HttpBridge` sends an `Idempotency-Key` header and accepts reconciliation and compensation URLs. For other providers, pass a stable idempotency key to the provider through your integration.

## Storage and optional integrations

The runtime has no required third-party dependencies. SQLite WAL is the default durable local store. Install optional extras when using the bundled distributed-store adapters:

```bash
python -m pip install -e ".[postgres,redis]"
```

`PostgresCausalStore` takes a DB-API connection factory. `RedisCausalStore` takes a Redis-compatible client. The application owns the lifetime and configuration of those connections.

## Run the verification suite

```bash
python -W error::ResourceWarning tests/test_runner_unittest.py
python -m compileall -q continuity tests
```

The randomized 10,000-trial campaign is a simulation and regression gate; it does not prove behavior for every external service or production failure mode. PostgreSQL and Redis adapters should be integration-tested against the services and configurations you plan to use.

## Operational notes

- Set a stable `run_id` when work must recover across process restarts.
- Provide reconcilers or compensators for effects that may have reached an external service before an error or crash.
- Active step leases renew while work runs. Stores do not issue fencing tokens, so idempotency and reconciliation remain important during long host pauses or storage outages.
- Trace export sends data only when `export_otlp_traces()` is explicitly called.

## Project information

- **Inventor:** Aryan Dev Singh Mankotia
- **License:** Apache-2.0
- **Python:** 3.10+

See [AUDIT.md](AUDIT.md) for the local audit findings and verification details, and [CHANGELOG.md](CHANGELOG.md) for changes in this working build.
