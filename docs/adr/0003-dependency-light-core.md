# ADR 0003: Keep the runtime core dependency-light

- **Status:** Accepted
- **Date:** 2026-11-19

## Context

Workflow users may need only the local runtime, while PostgreSQL, Redis, telemetry, and framework integrations have distinct dependencies and release cycles.

## Decision

Keep mandatory project dependencies empty. Use Python's standard library for the core runtime and isolate backend clients and framework packages as optional extras or separately versioned integrations.

## Consequences

The smallest installation remains lightweight. Optional paths require their own compatibility ranges and tests. The presence of an adapter in source or a mock test is not evidence of compatibility with a live service or current framework release.
