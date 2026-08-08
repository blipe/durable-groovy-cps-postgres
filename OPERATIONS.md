# Operations

## Health

`engine.administration().health()` returns:

- `UP`: engine running, store reachable, schema current, no dead-lettered workflows
- `DEGRADED`: engine quiescing, schema migration pending, or at least one workflow dead-lettered
- `DOWN`: engine closed or store health query failed

The response includes lifecycle state, schema status, counts by workflow status, active polls, active local drivers, and in-flight local handlers. These local counters are diagnostic, not a global cluster count.

Recommended mapping:

- liveness: process and scheduler are running
- readiness: `storeReachable == true`
- operational alert: `status == DEGRADED` or dead-letter count above the service's threshold

## Deployment and shutdown

Recommended PostgreSQL construction:

```java
DurableWorkflowEngine.builder(store)
        .schemaPreflight(SchemaPreflightMode.MIGRATE)
        .definitionPreflight(DefinitionPreflightMode.FAIL)
        .maximumInFlightSteps(256)
        .shutdownTimeout(Duration.ofSeconds(20))
        .build();
```

For a rolling termination, call `quiesce()` first, remove the instance from service traffic, then call `closeGracefully(timeout)`. Quiescing cancels the local poll task and rejects new workflow starts but permits already active handlers and continuation drives to complete. A timed-out handler is not declared complete; its durable lease expires and another node may claim it with a higher fencing token. Queued handlers re-check workflow state, deadline, and lease loss before entering application code.

The engine uses three independent schedulers:

- poll discovery
- handler invocation and completion callbacks
- lease renewal

Do not supply the same executor object for these roles in production.

## Schema migrations

`JdbcWorkflowStore` targets schema version 3. Migration history is stored in `durable_schema_history` with immutable checksums. Startup modes are:

- `OFF`: external ownership; no engine validation
- `WARN`: report pending/incompatible schema but construct the engine
- `FAIL`: require the exact target version before polling
- `MIGRATE`: serialize and apply pending migrations, then require compatibility

PostgreSQL migration uses a transaction-scoped advisory lock, making concurrent instance startup safe. Never edit an existing `VNNN` migration after release; add a new ordered migration.

## Triage sequence

1. Call `administration().diagnose(workflowId)` to inspect status, snapshot envelope metadata, exact-definition availability, pending step, and history.
2. Verify the exact registered definition version exists.
3. Inspect the matching `durable_step_invocation` by invocation ID when the workflow waits on a step.
4. Reconcile the external side effect using the stable invocation ID.
5. Choose one explicit action:
   - `retry`: rerun the durable operation
   - `resumeWithValue`: accept an externally verified result
   - `resumeWithFailure`: inject a failure into the script so its `try/catch` path runs
   - `cancel`: terminate without compensation
6. Record a specific operator reason; it is written to workflow history.

## Recovery semantics

### Retry

For a dead-lettered step, retry:

- retains the CPS continuation
- retains the durable command sequence
- requeues the step with attempt zero
- clears result/failure and active lease data
- preserves the invocation ID

Therefore retry can repeat the external side effect unless the handler deduplicates by invocation ID.

For a timer or signal wait, retry restores the matching `WAITING` state. For a dead-lettered `READY` continuation, retry restores its saved resume value and drives it again.

### Forced value

`resumeWithValue` records the pending step as succeeded when one exists, stores `ResumeValue.Success`, and resumes the continuation. Use it only after independently proving the external operation's result.

### Forced failure

`resumeWithFailure` records the pending step as failed when one exists and throws the supplied failure back into the CPS script. This permits workflow-level `try/catch` or cleanup logic to execute.

### Cancellation

Cancellation atomically marks a pending PostgreSQL step `CANCELLED`. A handler already executing may still finish its external side effect, but its stale completion cannot advance the cancelled workflow. External compensation remains an application responsibility.

## Metrics

Useful alerts:

```text
increase(durable_workflow_events_total{event="dead_lettered"}[5m]) > 0
increase(durable_step_events_total{event="failed"}[5m]) > 0
increase(durable_engine_events_total{event="lease_lost"}[5m]) > expected_takeover_rate
```

Exact exported names depend on the Micrometer registry's naming convention. Do not add workflow IDs, invocation IDs, customer IDs, or exception messages as metric tags.

## Logging

`LoggingWorkflowListener` emits one structured key/value-style message for each workflow, step, and engine event using `java.util.logging`. Applications may replace it with a listener backed by SLF4J or their corporate logging API.

Workflow input, step arguments, signal payloads, and outputs are intentionally not logged by the bundled listener.

## Tracing

The OpenTelemetry integration persists only propagation text fields, not live span objects. This avoids serializing SDK implementation state. Lifecycle spans created after restart use the restored propagation context as parent.

The bundled listener creates event spans rather than one permanently open span for a workflow that may last months. This keeps exporter and retention behavior predictable.

## Database inspection

Primary operational indexes are on:

- workflow status and `available_at`
- workflow lease expiry
- step status and `available_at`
- step lease expiry/deadline
- signal workflow/name/position

Do not mutate serialized blobs manually. Use the administration API so workflow and step rows change transactionally and history is retained.

## Definition deployment rule

Never remove a definition version while persisted workflows reference it. Deploy new and old definitions together, wait for the old active count to reach zero, then remove the old version in a later release.

A missing exact definition dead-letters the workflow instead of repeatedly failing the poll loop. Restoring the definition and invoking `retry` resumes the saved continuation.

## Startup gate

Production deployments should use `DefinitionPreflightMode.FAIL`. A deployment then refuses to begin polling when active or recoverable workflows depend on an absent exact source version. Use `administration().verifyDefinitions()` before removing old definitions from an application build.

## Snapshot policy rollout

Pass the same `JavaObjectCodec` instance or equivalent `DeserializationPolicy` to both `JdbcWorkflowStore` and `DurableWorkflowEngine`. Add application package prefixes before upgrading existing workflows containing those value types. A rejected class dead-letters continuation execution rather than silently substituting another type.

## Retention job

Run archival from one or more schedulers in bounded batches. Database row locking serializes overlapping candidates; repeated requests are safe because archived workflows no longer exist in the active table. Keep `DEAD_LETTERED` out of routine retention until an operator explicitly decides recovery is no longer required.

Example daily policy:

```java
while (administration.archive(
        ArchiveRequest.completedBefore(Instant.now().minus(Duration.ofDays(30)))
                .withLimit(1_000)).count() > 0) {
    // Continue bounded batches.
}
```


## Global dependency limits

Use `stepConcurrencyLimit(name, maximum)` for dependencies with hard shared capacity. The configured value is persisted and must match on every replica. A configuration change is therefore an explicit operational change: quiesce or roll out a release that uses the new agreed value after updating the row deliberately. Do not use this mechanism as a per-second rate limiter; it bounds concurrent live step leases.

## Stuck-work detection

Run `administration().stuckWork(overdueBy, limit)` or `scripts/admin.sh stuck`. Alert on non-empty reports after allowing at least several poll intervals and one lease duration. Signal waits are not stuck by themselves. Investigate the exact workflow with `diagnose`, then retry, force-resume, cancel, or dead-letter as appropriate.

## Encryption rollout and rotation

1. Deploy readers configured with `AesGcmObjectCodec`; they can still read plaintext legacy snapshots.
2. Keep the prior key IDs available while old snapshots remain active or archived.
3. Change the provider's active key to rotate writes.
4. Retire an old key only after no active or archived snapshot requires it, or after a controlled re-encryption job outside the engine.

Loss of a historical key makes the corresponding snapshot intentionally unreadable and should be treated like any other unrecoverable snapshot failure. Store keys in a KMS/HSM-backed provider; never in the workflow database.

## Scheduled retention

`RetentionPolicy` executes one bounded archival batch on a dedicated scheduler. Quiesce cancels future maintenance runs; graceful shutdown waits for an active batch until its single deadline. Keep archival batches small enough to avoid long database transactions.

## JDBC admin CLI

The bundled CLI is intentionally narrow and emits line-oriented JSON. It reads credentials from environment variables and supports `health`, `schema`, `limits`, `stuck`, and `archive`. It never executes continuations and therefore remains safe to run separately from application replicas.
