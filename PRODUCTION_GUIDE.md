# Production guide

CONTINUITY provides durable step history and recovery decisions. This branch adds token validation at the event append boundary. Run the backend service tests in CI and validate the storage backend and external-effect contract for your own workload. A local fencing token cannot revoke a remote request already sent by a stale worker; use external idempotency or downstream fencing as well.

## Choose a store

- **In-memory:** tests and short-lived processes only. State disappears with the process.
- **SQLite:** simple single-host deployments with durable local storage. The library enables WAL, `synchronous=FULL`, and a busy timeout. Keep the database on a supported local filesystem, monitor disk capacity, and include the database plus its WAL/checkpoint state in backup and restore procedures.
- **PostgreSQL:** optional distributed store. Configure the database, connection pool, credentials, backup, failover, and table prefix for your environment. CI integration tests use PostgreSQL 16; they do not establish your cluster's failover behavior.
- **Redis:** optional distributed store. Decide persistence, eviction, failover, namespace, and key retention policies. New keys use per-run hash tags for cluster scripts. Stop v0.8 writers during the one-time legacy event-key migration because they do not honor fencing tokens.

Install optional adapters from the repository checkout with `python -m pip install -e ".[postgres]"` or `python -m pip install -e ".[redis]"` as appropriate. The core package has no mandatory third-party dependency.

`PostgresCausalStore` needs a connection factory that returns transactional connections with autocommit disabled. Fenced append, acquisition, and renewal checks must hold their row locks through the corresponding write.

## External side effects

Declare the most conservative `SideEffectClass` that describes a step. For remote writes, pass a stable idempotency key to the service and implement reconciliation against the authoritative external state. Provide a compensator only when the action is genuinely reversible. An application-level label cannot enforce exactly-once behavior in a service that does not honor idempotency or expose reconciliation.

During recovery, do not assume that a timeout means the remote operation failed. A remote service may have applied the operation and lost its response. Use reconciliation or stop for operator review when the result is uncertain.

## Operational controls

- Keep stable run IDs across restart and use stable step IDs for the same logical operation.
- Back up and test restoring persistent stores. Treat event histories as business data.
- Restrict database access to the application and authorized operators.
- Monitor process health, storage availability, disk capacity, queue/worker health in the host system, and recovery escalations.
- Keep idempotency keys, credentials, and payloads out of general-purpose logs and dashboards.
- Define application-level authentication, tenant separation, and operator authorization outside the current library.
- Configure `operator_authorizer` before using `ContinuityRuntime.approve_resume`; never treat the durable approval event as a substitute for the host application's identity and access policy.
- Pin the package and optional backend versions; rehearse upgrades against a copy of production data.

## Readiness checklist

Before relying on a deployment, run a failure exercise against its exact backend and external services: kill and restart workers, expire leases, repeat requests, lose responses, disconnect and restore storage, and verify that recovery either resumes safely or escalates. Record versions, topology, backup restoration results, and workload limits. The repository's randomized campaign and local tests are useful checks but are not a substitute for this deployment exercise.
