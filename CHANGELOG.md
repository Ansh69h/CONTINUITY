# Changelog

## Unreleased

- Improve the project README, GitHub repository metadata, and package keywords for accurate discovery.
- Credit Aryan Dev Singh Mankotia as the project inventor and add `CITATION.cff`.
- Halt when a step ID is reused with different inputs or side-effect semantics.
- Require reconciliation or compensation before retrying an uncertain compensatable effect.
- Make PostgreSQL lease acquisition conditional and atomic, and validate table prefixes.
- Make Redis lease renewal and release atomic owner checks.
- Renew active step leases and use unique per-execution lease owners.
- Close HTTP error responses after handling them.
- Add regression tests for the audited recovery and lease cases.
