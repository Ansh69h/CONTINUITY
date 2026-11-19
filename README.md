# CONTINUITY: Python Durable Workflow Recovery for AI Agents

[![CI](https://github.com/Ansh69h/CONTINUITY/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Ansh69h/CONTINUITY/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-orange.svg)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-0%20required-brightgreen.svg)](#storage-and-optional-integrations)
[![Verification](https://img.shields.io/badge/release%20gate-10%2C000%20trials%20passed-success.svg)](#formal-safety-invariants--chaos-verification)
[![Type Checked](https://img.shields.io/badge/typing-PEP%20561-blueviolet.svg)](continuity/py.typed)

**CONTINUITY is an ultra-lightweight, zero-dependency Python runtime for durable workflow execution and crash recovery.** Designed for AI agents, autonomous pipelines, and distributed tasks that interact with LLMs, APIs, databases, and microservices.

When your process dies mid-execution—due to a machine restart, out-of-memory kill (`SIGKILL`), unhandled exception, network timeout, or lost HTTP acknowledgement—CONTINUITY seamlessly resumes from where it left off, reusing committed outputs and applying deterministic recovery policies to incomplete steps.

---

## Why CONTINUITY?

Building reliable AI agents and long-running pipelines usually forces a difficult trade-off:
1. **Full-blown distributed workflow engines (e.g., Temporal, Cadence):** Require separate server clusters, daemons, dedicated infrastructure, and complex operational overhead.
2. **Ad-hoc retry loops & checkpoint dicts:** Miss edge cases like lost acknowledgements, split-brain executions, non-idempotent duplicate charges, and in-flight crashes.

CONTINUITY bridges this gap: **embedded, zero-infrastructure durable execution in pure standard-library Python**, powered by SQLite WAL causal logging, atomic step leasing, and a formal side-effect recovery engine.

### How CONTINUITY Compares

| Feature | CONTINUITY | Temporal / Cadence | LangGraph Checkpointing | Celery / RQ |
| :--- | :---: | :---: | :---: | :---: |
| **External Server Required** | **No (Embedded)** | Yes (Server Cluster) | No (Embedded) | Yes (Broker + DB) |
| **Required Dependencies** | **0 (Standard Library)** | Heavy Client SDK | Heavy (LangChain) | Redis / RabbitMQ |
| **Storage Options** | SQLite WAL, Postgres, Redis | Postgres / Cassandra | SQLite / Memory | Redis / RabbitMQ |
| **Side-Effect Taxonomy** | `PURE`, `IDEMPOTENT`, `COMPENSATABLE`, `IRREVERSIBLE` | Activity Retry Policies | Manual Node Handling | Retry backoff |
| **External Reconciliation** | **Built-in First-Class** | Manual Workflow Code | None | None |
| **Formal Invariants Checked** | **Yes (7 Invariants)** | No (Platform SLA) | No | No |
| **Deterministic Chaos Gate** | **10,000 Trial Simulation** | Integration Tests | Unit Tests | Unit Tests |

---

## Architecture

CONTINUITY operates on a causal event log. Every step execution emits deterministic causal events that track intent, execution start, failures, compensations, and commits.

```mermaid
flowchart TD
    A[Start Step] --> B{Step In Causal Log?}
    B -->|Committed| C[Reuse Cached Output]
    B -->|Not Found| D[Acquire Exclusive Lease]
    B -->|Failed / Incomplete| E[Analyze Side-Effect Class]
    
    E -->|PURE| D
    E -->|IDEMPOTENT| F{In-Flight Crash?}
    F -->|Yes + Reconciler| G[Reconcile External State]
    F -->|No| D
    E -->|COMPENSATABLE| H{Compensator or Reconciler?}
    H -->|Has Compensator| I[Compensate Previous Effect] --> D
    H -->|Has Reconciler| G
    H -->|None| J[Halt & Escalate for Operator Review]
    E -->|IRREVERSIBLE| K{Has Reconciler?}
    K -->|Yes| G
    K -->|No| J
    
    G -->|State Found| C
    G -->|State Not Found| D
    
    D --> L[Emit STEP_INTENT & STEP_STARTED]
    L --> M[Execute Callable]
    M -->|Success| N[Emit STEP_COMMITTED & Release Lease]
    M -->|Exception| O[Emit STEP_FAILED & Release Lease]
```

---

## The 4 Side-Effect Classes

Workflows interact with the outside world in fundamentally different ways. CONTINUITY requires each step to explicitly declare its side-effect characteristics:

| Side-Effect Class | Real-World Agent Examples | Recovery & Crash Behavior |
| :--- | :--- | :--- |
| `PURE` | Prompt formatting, math, data parsing, token counting | Safe to re-execute without risk. Incomplete attempts are transparently re-run. |
| `IDEMPOTENT` | Document search, GET requests, vector lookups, PUT/UPSERT | Retried with identical `Idempotency-Key`. Reconciler probes external state if interrupted in-flight. |
| `COMPENSATABLE` | Stock reservation, workspace provisioning, database locks | Uncertain in-flight attempts run compensating actions (e.g., release reservation) before re-trying. |
| `IRREVERSIBLE` | Credit card charge, sending email/SMS, external webhook | **Never** blindly retried after a start. Must provide proof via external reconciler; otherwise halts for human review. |

---

## Quick Start

### Installation

Install via pip (zero dependencies required):

```bash
pip install continuity-runtime
```

Or clone and install in development mode:

```bash
git clone https://github.com/Ansh69h/CONTINUITY.git
cd CONTINUITY
pip install -e ".[dev]"
```

### Basic Step Execution with SQLite WAL

```python
from continuity import ContinuityRuntime, SQLiteCausalStore, SideEffectClass

# 1. Initialize crash-safe WAL store
store = SQLiteCausalStore("workflow.db")

# 2. Bind runtime to a stable run_id
runtime = ContinuityRuntime(store=store, run_id="invoice-run-2042")

def calculate_totals(items: list[dict]) -> dict:
    subtotal = sum(item["price"] * item["qty"] for item in items)
    return {"subtotal": subtotal, "tax": round(subtotal * 0.08, 2)}

# 3. Execute step (committed outputs are durably persisted)
result = runtime.step(
    calculate_totals,
    [{"name": "GPU Server", "price": 120.0, "qty": 2}],
    step_id="calc-totals",
    side_effect=SideEffectClass.PURE,
)
print(result)  # {'subtotal': 240.0, 'tax': 19.2}
```

If your process crashes or restarts anytime later:
```python
# Re-running with the same run_id and step_id returns the committed output instantly
# without re-executing calculate_totals()!
result = runtime.step(
    calculate_totals,
    [{"name": "GPU Server", "price": 120.0, "qty": 2}],
    step_id="calc-totals",
    side_effect=SideEffectClass.PURE,
)
```

> [!NOTE]
> If step inputs or side-effect semantics change on recovery, CONTINUITY refuses to return stale data and halts with `EscalationRequiredError` to safeguard state integrity.

---

## Advanced Usage

### Decorators (`@recoverable` and `@recoverable_async`)

```python
import asyncio
from continuity import ContinuityRuntime, SQLiteCausalStore, SideEffectClass, recoverable_async

store = SQLiteCausalStore("agent.db")
runtime = ContinuityRuntime(store=store, run_id="agent-run-101")

@recoverable_async(runtime, side_effect=SideEffectClass.IDEMPOTENT, step_id="fetch-user-data")
async def fetch_user_data(user_id: str) -> dict:
    await asyncio.sleep(0.01)
    return {"user_id": user_id, "tier": "enterprise"}

@recoverable_async(runtime, side_effect=SideEffectClass.PURE, step_id="generate-summary")
async def generate_summary(data: dict) -> str:
    return f"Summary for {data['user_id']} ({data['tier']})"

async def main():
    user = await fetch_user_data("usr_42")
    summary = await generate_summary(user)
    print(summary)

asyncio.run(main())
```

### Idempotent HTTP Bridge with Reconciliation & Compensation

`HttpBridge` automatically injects `Idempotency-Key`, `X-Continuity-Run-Id`, and `X-Continuity-Step-Id` headers and provides external reconciliation:

```python
from continuity import ContinuityRuntime, SQLiteCausalStore, HttpBridge, SideEffectClass

store = SQLiteCausalStore("bridge.db")
runtime = ContinuityRuntime(store=store, run_id="order-job-901")
bridge = HttpBridge(runtime=runtime, timeout=5.0)

# Executes an irreversible payment with automatic reconciliation if network drops
response = bridge.request(
    step_id="charge-card",
    method="POST",
    url="https://api.payments.com/v1/charges",
    payload={"amount": 2500, "currency": "usd"},
    side_effect=SideEffectClass.IRREVERSIBLE,
    reconcile_url="https://api.payments.com/v1/reconcile",
    compensate_url="https://api.payments.com/v1/refund",
)
```

---

## Storage Backends

CONTINUITY includes built-in local stores and adapters for distributed stores:

### 1. SQLite WAL Store (Default / Embedded)
- Crash-safe across process `SIGKILL` and sudden power loss.
- Configured with `PRAGMA journal_mode=WAL`, `PRAGMA synchronous=FULL`, and `PRAGMA busy_timeout=30000`.
- Zero operational dependencies.

### 2. PostgreSQL Store (Distributed)
Install the postgres extra:
```bash
pip install "continuity-runtime[postgres]"
```
```python
import psycopg
from continuity import PostgresCausalStore

store = PostgresCausalStore(conn_factory=lambda: psycopg.connect("postgresql://user:pass@localhost/db"))
store.ensure_schema()
```
- Atomic conditional upserts with `RETURNING worker_id`.
- Table prefix sanitization and concurrency mapping.

### 3. Redis Store (Distributed)
Install the redis extra:
```bash
pip install "continuity-runtime[redis]"
```
```python
import redis
from continuity import RedisCausalStore

client = redis.Redis.from_url("redis://localhost:6379/0")
store = RedisCausalStore(client, namespace="continuity")
```
- Atomic Lua CAS scripts for sequence validation, lease renewal, and atomic owner release.

---

## Formal Safety Invariants & Chaos Verification

CONTINUITY is verified against **7 formal safety invariants**:

1. **INV-1: Monotonicity & Parent Integrity** — Strictly contiguous `1..N` sequence numbers with valid causal parent references.
2. **INV-2: Intent-Before-Effect Ordering** — Every `STEP_STARTED` or `STEP_COMMITTED` must be preceded by a matching `STEP_INTENT`.
3. **INV-3: Committed Output Immutability** — Once `STEP_COMMITTED` is recorded, output cannot mutate; duplicate commits are forbidden.
4. **INV-4: Irreversible Side-Effect Safety** — An `IRREVERSIBLE` step never executes more than once without verified external reconciliation proof.
5. **INV-5: Compensatable Crash Recovery Discipline** — A `COMPENSATABLE` step with an uncommitted start cannot retry until compensated or reconciled.
6. **INV-6: Idempotency Key Stability** — All events belonging to the same `step_id` carry an identical idempotency key.
7. **INV-7: Bounded Recovery Termination** — No step exceeds configured `max_attempts` without halting/escalating safely.

### The 10,000-Trial Release Gate

Run the full verification suite locally:

```bash
python -W error::ResourceWarning tests/test_runner_unittest.py
```

The test harness runs:
- **10,000-Trial Randomized Chaos Simulation:** Injects randomized faults (crashes mid-step, timeouts, lost acknowledgements) with zero invariant violations.
- **Process-Kill Verification:** Spawns a real child process that mutates external files and calls `os._exit(137)` before commit, testing recovery.
- **Multi-Worker Contention:** 12 concurrent workers contending on the same SQLite WAL database step.
- **Adversarial HTTP Mock:** Real HTTP server injecting lost acknowledgements, duplicate requests, and 503s.

---

## OpenTelemetry & APM Traces

Export execution spans to any OTLP-compatible collector (Jaeger, Grafana Tempo, Datadog):

```python
from continuity import export_otlp_traces

events = store.get_events(runtime.run_id)
status_code = export_otlp_traces(events, endpoint="http://localhost:4318/v1/traces")
```

---

## Project Information & Citation

- **Project Inventor:** Aryan Dev Singh Mankotia
- **Maintainer:** [Ansh69h](https://github.com/Ansh69h)
- **License:** Apache-2.0
- **Python Compatibility:** Python 3.10, 3.11, 3.12, 3.13, 3.14

If you use CONTINUITY in academic research or engineering systems, please cite it using [CITATION.cff](CITATION.cff):

```bibtex
@software{mankotia2026continuity,
  author = {Mankotia, Aryan Dev Singh},
  title = {CONTINUITY: Python Durable Workflow Recovery for AI Agents},
  version = {0.8.0},
  url = {https://github.com/Ansh69h/CONTINUITY},
  license = {Apache-2.0}
}
```
