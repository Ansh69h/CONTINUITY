# ADR 0001: Version persisted causal events

- **Status:** Implemented; local PostgreSQL and SQLite migration tests passed
- **Date:** 2026-11-19

## Context

The v0.8 event dictionary and SQLite/PostgreSQL tables do not carry an explicit schema version. Readers infer the shape from field names and defaults, which makes future migrations and rejection of unknown formats ambiguous.

## Decision

Add an integer schema version to serialized events. Treat records without the field as the v0.8 legacy format. Readers must reject unsupported future versions clearly. Migrate existing database tables transactionally and preserve existing event contents.

## Consequences

This improves format identification and makes unsupported versions fail explicitly. SQLite and PostgreSQL migrations and Redis version-aware reads are implemented. Local SQLite and PostgreSQL 16.16 migration tests pass. Backups remain recommended before upgrading persisted data.
