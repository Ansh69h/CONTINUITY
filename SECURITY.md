# Security policy

## Supported versions

Security support and patch windows are not yet defined. The project is early-stage; users should pin a reviewed version and assess changes before upgrading. Do not assume that a release has undergone an independent security audit.

## Reporting a vulnerability

Please do not publish exploit details in a public issue. If GitHub private vulnerability reporting is enabled for this repository, use the repository's **Security → Advisories → Report a vulnerability** flow. Otherwise contact the repository owner privately through the GitHub profile at [github.com/Ansh69h](https://github.com/Ansh69h) and include affected versions, impact, and a minimal reproduction. Please allow time for triage before public disclosure.

## Security boundaries and current limitations

- Continuity does not authenticate users, authorize operators, or isolate tenants.
- Event payloads can contain step arguments and outputs. Treat the store as sensitive application data; the library does not encrypt payloads or automatically redact secrets.
- OTLP export omits payloads and raw run/step identifiers by default. Deterministic trace/span IDs still correlate repeated exports of the same run. `include_sensitive_attributes=True` adds raw run/step identifiers and step names; idempotency keys are never exported. Only opt in when the collector is approved for those identifiers.
- The production-readiness branch validates fencing tokens atomically at event append across its stores. PostgreSQL 16.16 and Memurai 4.1.7 service tests passed locally; upstream Redis 7 CI verification is pending. A fencing token does not stop an already-issued external request; downstream idempotency or token enforcement is still required.
- The HTTP bridge accepts HTTP(S) URLs by default. Configure its exact-host `allowed_hosts` list when URLs can be influenced by users; redirects are checked against the same list. Responses are capped at 1 MiB by default. The bridge does not provide application authentication or DNS-rebinding protection, so apply network egress controls for untrusted destinations.

## Deployment precautions

Use least-privilege database credentials, isolate worker networks, secure backups and telemetry, limit outbound HTTP destinations, and keep credentials out of step inputs and payloads where possible. Apply tenant and operator access controls in the host application. Report suspected security issues privately using the channel above.
