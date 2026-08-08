# 0.6.0 production completion

## Added

- Database-global named step concurrency limits with replica configuration mismatch detection.
- Migration V003 and an index for active same-name step claims.
- Stuck-work reports for aged READY workflows, overdue timers, abandoned claimable steps, and expired/deadline-exceeded running steps.
- Periodic bounded retention on a dedicated maintenance scheduler with graceful-drain accounting.
- AES-GCM authenticated snapshot encryption with persisted key IDs, historical-key rotation, tamper detection, and plaintext rolling-upgrade reads.
- Environment-configured PostgreSQL admin CLI for health, schema, limits, stuck work, and archival.
- Complete `ProductionExample` assembly.
- A 0.6.0 compatibility fixture and regression coverage for the 0.3, 0.5, and 0.6 formats.

## Delivery semantics unchanged

- Step handlers remain at least once.
- `invocationId` remains the idempotency key.
- Script code between durable boundaries must remain deterministic and side-effect free.
- Exact definition ID, version, and source hash are required for continuation resume.

## Still deliberately deferred

- External/remote worker transport.
- Parallel branches and child workflows.
- Saga/compensation DSL.
- Continuation/source migration.
- Sandboxed untrusted Groovy.
- Language-neutral continuation encoding.
- Exactly-once guarantees for remote systems.
