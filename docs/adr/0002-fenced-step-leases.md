# ADR 0002: Fence lease holders at the append boundary

- **Status:** Implemented; upstream Redis 7 CI verification pending
- **Date:** 2026-11-19

## Context

The current runtime renews and checks a lease separately from appending the next event. A process can pause after a successful check, lose its lease, and resume after a different worker takes over. Event sequence checks alone do not prove that the appending worker still owns the step.

## Decision

Represent a successful acquisition as an immutable lease containing run ID, step ID, owner ID, and a monotonically increasing fencing token. Renewals preserve the token. Reacquisition increments it. Each protected append validates owner, token, and lease freshness atomically with sequence validation in the store transaction or script.

The store rejects stale lease tokens at the protected append boundary. This does not by itself fence an external service: after worker B acquires a newer lease, the external resource may still accept worker A's request until it has registered and begun enforcing B's newer token. A token cannot undo an effect the external service already accepted. Before retrying an uncertain effect, require target-enforced idempotency or reconcile the external state; use resource-side fencing when stale writes must be rejected.

## Consequences

Stores use persistent token counters and migrations, and append checks are atomic in the store transaction or script. Custom store implementations must add fenced lease acquisition, renewal, release, and an `append_event(..., lease=...)` check. Local service tests pass on PostgreSQL 16.16 and Memurai 4.1.7; upstream Redis 7 service verification remains pending in CI. v0.8 does not provide this guarantee.
