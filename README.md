# CONTINUITY: Durable Python Workflow Engine for AI Agents

[![CI](https://github.com/Ansh69h/CONTINUITY/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Ansh69h/CONTINUITY/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-orange.svg)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-0%20required-brightgreen.svg)](#storage-and-optional-integrations)
[![Verification](https://img.shields.io/badge/release%20gate-10%2C000%20trials%20passed-success.svg)](#safety-invariants--reliability-testing)
[![Type Checked](https://img.shields.io/badge/typing-PEP%20561-blueviolet.svg)](continuity/py.typed)

**Build long-running Python agents that can recover, resume, and handle retries safely.**

CONTINUITY is a dependency-light Python durable workflow engine for AI agents, automation, and distributed jobs. It records versioned causal progress, reuses committed outputs, and applies explicit recovery rules after process crashes, timeouts, or lost acknowledgements. It provides durable step execution and recovery primitives; it is not a workflow scheduler or distributed transaction coordinator.

- **Resume after failure:** Continue from a durable SQLite WAL event log.
- **Retry with clear rules:** Declare whether a step is pure, idempotent, compensatable, or irreversible.
- **Protect external side effects:** Reconcile uncertain calls, compensate when possible, or stop for review.
- **Scale storage when needed:** Use SQLite by default, with optional PostgreSQL and Redis backends.
- **Start with no required dependencies:** The core runtime uses the Python standard library.

---

## Why CONTINUITY?

Long-running agents face two common failure modes: a service can process a request even when its response is lost, and a process can stop between an external side effect and its checkpoint. A plain retry loop may repeat work; a hand-built checkpoint often lacks enough context to decide what is safe.

CONTINUITY persists workflow progress locally and uses each step's declared side-effect class to choose how to recover. The default SQLite WAL store needs no separate server; PostgreSQL and Redis are available for distributed deployments.

### Durable Workflow Features

| Workflow need | CONTINUITY approach |
| :--- | :--- |
| Resume completed work | Persist committed outputs in a causal event log and reuse them on replay. |
| Recover uncertain side effects | Declare a step pure, idempotent, compensatable, or irreversible; reconcile or stop when state is uncertain. |
| Run locally or across workers | Start with embedded SQLite WAL, then add PostgreSQL or Redis storage when needed. |
| Keep setup small | The core package uses the Python standard library; integrations are optional extras. |

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

Install the current project directly from GitHub:

```bash
python -m pip install "git+https://github.com/Ansh69h/CONTINUITY.git"
```

Or clone it and install the development tools:

```bash
git clone https://github.com/Ansh69h/CONTINUITY.git
cd CONTINUITY
python -m pip install -e ".[dev]"
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
bridge = HttpBridge(runtime=runtime, timeout=5.0, allowed_hosts=["api.payments.com"])

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

The HTTP bridge requires HTTP or HTTPS, rejects embedded URL credentials, supports an optional exact-host allowlist (checked after redirects too), and limits response bodies to 1 MiB by default. Add every intended reconciliation and compensation host to `allowed_hosts`.

---

## Storage Backends

CONTINUITY includes built-in local stores and adapters for distributed stores:

### 1. SQLite WAL Store (Default / Embedded)
- Durable local storage configured with SQLite WAL and `synchronous=FULL`; actual durability still depends on the host filesystem, operating system, and storage device.
- Configured with `PRAGMA journal_mode=WAL`, `PRAGMA synchronous=FULL`, and `PRAGMA busy_timeout=30000`.
- Zero operational dependencies.

### 2. PostgreSQL Store (Distributed)
From the repository checkout, install the PostgreSQL extra:
```bash
python -m pip install ".[postgres]"
```
```python
import psycopg
from continuity import PostgresCausalStore

store = PostgresCausalStore(conn_factory=lambda: psycopg.connect("postgresql://user:pass@localhost/db"))
store.ensure_schema()
```
- Fenced leases with monotonically increasing tokens and append-time owner checks.
- Schema setup migrates v0.8 event and lease tables. CI includes service-backed tests; validate your own failover, backup, and recovery configuration before production use.

### 3. Redis Store (Distributed)
From the repository checkout, install the Redis extra:
```bash
python -m pip install ".[redis]"
```
```python
import redis
from continuity import RedisCausalStore

client = redis.Redis.from_url("redis://localhost:6379/0")
store = RedisCausalStore(client, namespace="continuity")
```
- Lua scripts atomically acquire, renew, and release fenced leases and validate the token while appending events.
- New Redis keys use a run hash slot for Redis Cluster. Stop v0.8 writers during key migration because those workers do not enforce fencing tokens.

## Optional Framework Adapters

Install only the adapter you use; the runtime core remains dependency-free:

```powershell
python -m pip install -e "integrations[langgraph]"
python -m pip install -e "integrations[openai-agents]"
python -m pip install -e "integrations[fastapi]"
```

The separate `continuity-integrations` package includes a generic callable wrapper, a LangGraph node wrapper, a local OpenAI Agents SDK function-tool wrapper, and an authorization-hook-based FastAPI router. These adapters wrap local step execution. LangGraph retains its own checkpointer, the Agents SDK retains its own session/run state, and FastAPI authentication remains application-owned. See [integrations/README.md](integrations/README.md) and its examples for version ranges and limitations.

---

## Safety Invariants & Reliability Testing

CONTINUITY checks **7 safety invariants** across its event log and recovery paths:

1. **INV-1: Monotonicity & Parent Integrity** — Strictly contiguous `1..N` sequence numbers with valid causal parent references.
2. **INV-2: Intent-Before-Effect Ordering** — Every `STEP_STARTED` or `STEP_COMMITTED` must be preceded by a matching `STEP_INTENT`.
3. **INV-3: Committed Output Immutability** — Once `STEP_COMMITTED` is recorded, output cannot mutate; duplicate commits are forbidden.
4. **INV-4: Irreversible Retry Authorization** — An `IRREVERSIBLE` step may retry only after verified external reconciliation or durable authorized operator approval; `max_attempts` still bounds execution.
5. **INV-5: Compensatable Crash Recovery Discipline** — A `COMPENSATABLE` step with an uncommitted start cannot retry until compensated or reconciled.
6. **INV-6: Idempotency Key Stability** — All events belonging to the same `step_id` carry an identical idempotency key.
7. **INV-7: Bounded Recovery Termination** — No step exceeds configured `max_attempts` without halting/escalating safely.

### The 10,000-Trial Release Gate

Run the full pytest and reliability suite locally:

```bash
python -W error::ResourceWarning -m pytest -q
```

The test harness runs:
- **10,000-Trial Randomized Model Campaign:** Injects randomized faults and checks the model with sampled SQLite runs. It is not 10,000 live database trials or a formal correctness proof.
- **Process-Kill Verification:** Spawns a real child process that mutates external files and calls `os._exit(137)` before commit, testing recovery.
- **Multi-Worker Contention:** 12 concurrent workers contending on the same SQLite WAL database step.
- **Adversarial HTTP Mock:** Real HTTP server injecting lost acknowledgements, duplicate requests, and 503s.

---

## OpenTelemetry & APM Traces

Export workflow and step spans to any OTLP-compatible collector (Jaeger, Grafana Tempo, Datadog). Spans use lifecycle timestamps; raw payloads and identifying attributes are omitted by default.

```python
from continuity import export_otlp_traces

events = store.get_events(runtime.run_id)
status_code = export_otlp_traces(events, endpoint="http://localhost:4318/v1/traces")
```

Set `include_sensitive_attributes=True` only when the collector is approved for run IDs, step IDs, and step names. Trace/span IDs are deterministic and still link repeated exports of the same run. Idempotency keys, event arguments, and outputs are never added to telemetry spans.

## Local CLI

The `continuity` command can list runs, inspect events, show recovery decisions, verify the seven recorded-history invariants, and invoke an application handler through the runtime planner:

```bash
continuity runs list --database workflow.db
continuity runs inspect --database workflow.db --run-id invoice-run-2042
continuity runs recoveries --database workflow.db --run-id invoice-run-2042
continuity runs verify --database workflow.db --run-id invoice-run-2042
```

Inspection redacts event payloads by default. Use `--include-payload` only when the terminal is safe for stored workflow inputs and outputs. `runs resume` requires an explicit application callable; uncertain irreversible work still escalates unless a configured operator authorization hook records an approval.

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
  title = {CONTINUITY: Durable Python Workflow Engine for AI Agents},
  version = {0.8.0},
  url = {https://github.com/Ansh69h/CONTINUITY},
  license = {Apache-2.0}
}
```
