# Local audit notes

## Scope and release choice

The supplied v0.5.0 and v0.8.0 archives contain different package layouts and
different runtime APIs. v0.8.0 is a rewrite, not a drop-in upgrade from v0.5.0.
The local project is based on v0.8.0; the original archives remain under the
parent `releases` directory for comparison.

## Findings corrected in the local project

- A committed step could return cached output after the caller changed its
  inputs, because replay was keyed only by `step_id`. The runtime now halts when
  persisted idempotency inputs or side-effect classification do not match.
- A failed `COMPENSATABLE` step was treated as safe to retry even though its
  external effect could have happened before the exception. Recovery now
  requires reconciliation or compensation for any started, uncompensated
  attempt.
- PostgreSQL lease acquisition used a read followed by an unconditional upsert,
  allowing concurrent workers to both acquire the lease. It now uses a
  conditional atomic upsert. Table prefixes are restricted to simple SQL
  identifiers, and unique-key races map to the store's concurrency error.
- Redis lease renewal and release used separate owner checks and mutations. Both
  operations now use atomic Lua scripts that compare the current owner.
- HTTP error responses were left open after handling. They are now closed, and
  the full suite passes with `ResourceWarning` promoted to an error.
- Non-JSON step outputs (such as `datetime`, `UUID`, dataclasses, sets) crashed
  the SQLite WAL and distributed stores during `json.dumps()`. The stores now
  use `safe_json_default`, enabling rich output serialization.
- Custom objects in step arguments previously serialized memory addresses
  (`0x...`) in `default=str`, causing false `EscalationRequiredError` input-drift
  halts across process restarts. `_compute_idempotency_key` now uses deterministic
  structural serialization.
- `HttpBridge.request` previously bypassed input drift detection by passing empty
  arguments to `runtime.step()`. It now properly forwards `method`, `url`, and
  `payload` into the idempotency key and execution step.
- Added `continuity/py.typed` (PEP 561) type marker for IDE and type checker compatibility.

## GitHub discovery metadata prepared

The local README, package description and keywords, author metadata, and
`CITATION.cff` now use accurate workflow-recovery search terms and credit Aryan
Dev Singh Mankotia as inventor. Apply this description and topic set in GitHub's
repository About settings after the repository is created:

```text
Description: Python durable workflow recovery runtime for AI agents, automation, and distributed jobs. Crash recovery, idempotent retries, causal event logs, SQLite WAL, PostgreSQL, and Redis.
Topics: python, workflow-engine, durable-execution, workflow-recovery, crash-recovery, fault-tolerance, ai-agents, agent-runtime, distributed-systems, idempotency, workflow-orchestration, reliability, sqlite, postgresql, redis, automation
```

GitHub repository topics can help people discover projects by purpose, subject,
and language. Search ranking is not guaranteed.

## Remaining risks

- Leases renew during a running step, and each step uses a unique lease owner.
  The stores do not issue fencing tokens, so a long host pause or prolonged
  storage outage can still let a lease expire while work is running. Continue
  to use idempotency keys, reconcilers, or compensators for external effects.
- PostgreSQL and Redis changes have local mock-level regression coverage, but
  were not exercised against live services.
- The 10,000-trial release gate is a deterministic randomized simulation. It is
  useful regression coverage, but does not prove correctness against every
  external system or production failure mode.
- Only Python 3.14.3 was available for local verification. The added CI matrix
  targets Python 3.10 through 3.14, but has not run on GitHub Actions yet.

## Local verification

Verified on this PC with Python 3.14.3:

```text
python -W error::ResourceWarning tests/test_runner_unittest.py
16 tests passed, including the 10,000-trial release gate and long-step lease
renewal regression.
python -m compileall -q continuity tests
passed
pip check
No broken requirements found.
```

Built and installed locally in `.venv`:

```text
dist/continuity_runtime-0.8.0-py3-none-any.whl
SHA256: 565b361f6b51097b7abdd7e0d73ca5106b046c0a869bf9dadefff59443b36304
```

The wheel smoke test imported `continuity` from `.venv` outside the source tree
and completed a sample step with result `42`. Nothing has been uploaded to
GitHub.
