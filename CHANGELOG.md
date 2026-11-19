# Changelog

## Unreleased

- Add `safe_json_default` deterministic serializer supporting datetimes, dataclasses, UUIDs, sets, and custom classes across storage engines and idempotency keys.
- Fix `HttpBridge.request` argument forwarding so HTTP method, URL, and payload are hashed into step idempotency keys.
- Add `py.typed` (PEP 561) type marker for IDE and type checker compatibility.
- Configure `pyproject.toml` pytest discovery to ignore runner script.
- Add regression tests for JSON payload serialization, custom class stability, and HTTP bridge input drift.
- Improve the project README, GitHub repository metadata, and package keywords for accurate discovery.
- Credit Aryan Dev Singh Mankotia as the project inventor and add `CITATION.cff`.
- Halt when a step ID is reused with different inputs or side-effect semantics.
- Require reconciliation or compensation before retrying an uncertain compensatable effect.
- Make PostgreSQL lease acquisition conditional and atomic, and validate table prefixes.
- Make Redis lease renewal and release atomic owner checks.
- Renew active step leases and use unique per-execution lease owners.
- Close HTTP error responses after handling them.
- Add regression tests for the audited recovery and lease cases.
