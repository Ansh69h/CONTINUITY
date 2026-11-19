# Recovery guide

CONTINUITY records a causal history for each run so a new runtime can decide whether to reuse a committed result, retry, reconcile, compensate, or escalate. Recovery is only as reliable as the event store and the external operation's idempotency/reconciliation contract.

## Step classes

- `PURE`: can be repeated without external effects.
- `IDEMPOTENT`: repeated execution must produce the same external effect; use the same key and ensure the service actually honors it.
- `COMPENSATABLE`: an uncertain action needs reconciliation or a successful compensator before a retry.
- `IRREVERSIBLE`: an uncertain action must not be blindly repeated; reconcile against external truth or halt for a human decision.

The runtime derives the idempotency key from the run, step, and call inputs. Reusing a step ID with changed inputs or effect semantics is treated as a conflict requiring escalation. Keep IDs and inputs stable across restart when replaying the same logical workflow.

## What to do after an interruption

1. Preserve the existing store and logs; do not delete incomplete events to force a retry.
2. Restart with the same store, run ID, step IDs, and inputs.
3. Inspect the returned result or `EscalationRequiredError` and compare event history with the external system.
4. For uncertain effects, use a reconciler that queries authoritative state with the stable idempotency key.
5. For compensatable work, verify the compensator's result before retrying.
6. If external state cannot be established, stop and escalate to an operator.

## Manual operator decisions

The library exposes `inspect_pending_recoveries()` and an optional `SideEffectPolicy` with `allow_operator_approval=True`. Configure a host `operator_authorizer` callback, then call `approve_resume()` with the original step inputs and a reason. The runtime checks the unchanged idempotency key, acquires a fenced lease, and stores a `RECOVERY_APPROVED` event. Calling `step()` again still goes through the planner, consumes that approval for one attempt, and obeys `max_attempts`. An approval cannot bypass a policy-required reconciler or compensator. The library does not supply user authentication; the host application must authenticate the operator.

## Evidence limits

The repository has invariant checks, randomized fault simulations, and local SQLite crash/contention tests. The 10,000-trial campaign is model-based and mostly uses in-memory storage; SQLite is sampled, not used for every trial. PostgreSQL 16.16 and Memurai 4.1.7 service tests passed locally; Memurai is Redis 7.2.11-compatible but is not the upstream Redis server. GitHub CI is configured to verify upstream Redis 7. No external exactly-once guarantee or universal resilience to network and clock failures is claimed.
