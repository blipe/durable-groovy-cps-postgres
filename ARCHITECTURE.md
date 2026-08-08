# Architecture

## Responsibilities

```text
api/
  workflow definitions, commands, step SDK, views

admin/
  bounded search, health, explicit recovery controls

observability/
  lifecycle listener SPI, logging, metrics, tracing, durable context propagation

runtime/
  Groovy CPS compilation, continuation driver, retries, timers, signals,
  dead-letter classification, lease renewal, failpoints

runtime/state/
  immutable serializable workflow state

store/
  local CAS stores
  coordinated store contract
  PostgreSQL implementation and schema
```

## Workflow state

```text
                  durable command
  READY ---------------------------------> WAITING
    |                                         |
    | script returns                          | step result / timer / signal
    v                                         v
COMPLETED / FAILED                         READY

READY or WAITING -- unrecoverable runtime fault --> DEAD_LETTERED
DEAD_LETTERED -- retry/forced resume -----------> READY or WAITING
READY / WAITING / DEAD_LETTERED -- cancel -----> CANCELLED
```

`READY` means a persisted resume value exists and the continuation may run.

`WAITING` means a continuation and pending durable command are committed.

`FAILED`, `COMPLETED`, and `CANCELLED` contain no continuation. `DEAD_LETTERED` intentionally retains the continuation and either its pending command or resume value.

## Failure boundary

The runtime distinguishes two classes:

1. **Workflow failure**: CPS execution returns an unhandled script exception. The workflow becomes terminal `FAILED`.
2. **Engine/delivery failure**: safe automatic progress is no longer possible. The workflow becomes recoverable `DEAD_LETTERED`.

Step retry exhaustion is an engine/delivery failure because an operator may reconcile the side effect and retry or inject a result. Definition/source mismatch and snapshot decoding failures are also dead-lettered rather than repeatedly poisoning the polling loop.

## Coordinated PostgreSQL model

The workflow snapshot remains one immutable logical record, but operational entities are normalized:

```text
durable_workflow
  CPS continuation, resume value, output/failure, revision,
  propagation context, indexed wait state, workflow lease and fencing token

         1
         |
         +---- * durable_signal
         |
         +---- * durable_step_invocation
                    lifecycle, attempt, schedule, deadline,
                    step lease and fencing token, result/failure

durable_idempotency
  optional handler-side transaction key and cached result
```

PostgreSQL transaction time is authoritative for lease and durable scheduling decisions. JVM clocks are not used to decide lease ownership.

## Suspension transaction

For:

```groovy
def result = step('send-email', request)
```

execution is:

```text
1. CPS reaches step(...).
2. DurableScript creates Step(sequence, name, argument, options).
3. Continuable.suspend yields the command.
4. Engine serializes the continuation positioned after step(...).
5. Under a fenced workflow lease, one transaction:
     update durable_workflow to WAITING
     insert durable_step_invocation as PENDING
     synchronize queued signals
     commit
6. Only after commit may a worker claim the step.
```

There is no state in which the workflow is durably waiting but its step row was not committed, or vice versa.

## Step claim

A claim transaction locks the invocation row and accepts it when:

- `PENDING` or `RETRY_WAIT` and `available_at <= current_timestamp`, or
- `RUNNING` but its lease expired, or
- `RUNNING` but its execution deadline expired.

The transaction increments attempt and fencing token, records owner/expiry/deadline, and commits `RUNNING`. The invocation ID does not change across attempts.

## Step completion transaction

Completion locks the step row and verifies owner, fencing token, lease expiry, and execution deadline.

```text
success:
  mark step SUCCEEDED with result
  move workflow to READY with ResumeValue.Success

retryable failure:
  mark step RETRY_WAIT with next available_at
  keep workflow WAITING with the same continuation and command

exhausted failure:
  mark step FAILED
  move workflow to DEAD_LETTERED while retaining continuation and command
```

The continuation is resumed only after a success transaction commits or after an explicit administrative recovery.

## Administrative transition transaction

For a workflow with a pending step, recovery changes workflow and step rows together:

```text
retry:
  DEAD_LETTERED -> WAITING
  FAILED/CANCELLED/RUNNING step -> PENDING

forced value:
  DEAD_LETTERED -> READY with ResumeValue.Success
  step -> SUCCEEDED

forced failure:
  DEAD_LETTERED -> READY with ResumeValue.Failure
  step -> FAILED

cancel or operator dead-letter:
  workflow -> CANCELLED or DEAD_LETTERED
  nonterminal pending step -> CANCELLED
```

This prevents a step poller from dispatching a command after an operator has quarantined or cancelled its workflow.

## Fencing

Every acquisition increments a persistent token:

```text
owner A: token 7
lease expires
owner B: token 8
owner A completion with token 7 -> rejected
```

Lease renewal does not change the token. Renewal runs on a dedicated scheduler so blocked polling or handler callbacks cannot starve the fencing heartbeat. Every live engine process must use a unique `instanceId`.

## Execution timeout

`StepOptions.executionTimeout` creates a durable deadline when the step is claimed.

After the deadline, renewal and completion with the old token fail and another node can claim the same invocation with a higher token. Interrupting a Java future remains best effort.

## Crash windows

### Before suspension commit

The previous workflow snapshot remains authoritative. Deterministic script code is replayed to the same command.

### After suspension commit but before claim

Another poller sees the `PENDING` step row and claims it.

### After claim but before handler call

The `RUNNING` lease eventually expires. Another node claims the invocation with a higher token.

### During handler execution

Lease renewal keeps ownership alive. Process death or deadline expiry makes the invocation claimable again.

### After external side effect but before completion commit

The handler can run again. This is the unavoidable at-least-once window unless the external side effect and deduplication record share a transaction.

### After completion commit but before CPS resume

The workflow is `READY` with the durable result. Recovery resumes without running the handler again.

### After CPS resume but before the next durable boundary

The prior `READY` record remains authoritative and the pure script segment is replayed.

## Database-local idempotency

`JdbcIdempotencyStore.executeOnce` inserts a reservation with `ON CONFLICT DO NOTHING` before invoking the callback. PostgreSQL unique-key locking makes concurrent calls wait for the owner transaction.

The owner callback receives the same JDBC connection. Its business writes, completed marker, and serialized result commit together. If the transaction rolls back, the reservation and business writes both disappear.

This only strengthens effects performed through that connection. External APIs and messaging systems remain at least once and must deduplicate the invocation ID themselves.

## Observability model

`WorkflowRecord.telemetryContext` stores immutable string propagation fields. It is captured at start and restored around:

- continuation execution
- local step invocation
- coordinated step invocation

Listeners receive immutable event records after durable transitions commit. Listener failures are isolated from workflow correctness.

Micrometer uses only bounded tags. OpenTelemetry creates lifecycle event spans parented to the persisted context rather than retaining one live span across long suspension periods.

## Local stores

`InMemoryWorkflowStore` and `FileWorkflowStore` implement revision CAS only. They retain single-node behavior and emulate administrative recovery in one workflow-record replacement.

The engine detects `CoordinatedWorkflowStore` and switches to indexed runnable queries, workflow leases, separate due-step claims, transactional suspension/completion/recovery, and database-authoritative time.

## Definition compatibility

A definition is identified by:

```text
id + explicit version + SHA-256(source)
```

Multiple versions can be registered simultaneously. New workflows use the highest version. Persisted continuations are decoded with the exact version classloader. A mismatch dead-letters before resume.

## Serialization

`JavaObjectCodec` serializes CPS snapshots and durable values. It enforces limits on bytes, depth, references, and array length, but Java serialization remains a trusted-storage format.

Signals and step records are serialized independently from the continuation so they can be queried and coordinated without decoding the workflow script object graph.

## Invariants

1. External effects occur only in step handlers.
2. Step handlers are at least once.
3. Invocation IDs remain stable across attempts, recovery, and restarts.
4. Workflow suspension and step creation are one transaction.
5. Step state and workflow resume/dead-letter state are one transaction.
6. Administrative step recovery and workflow recovery are one transaction.
7. A stale lease holder cannot commit after takeover.
8. Results are persisted before CPS resumes.
9. Continuations never resume against different source bytes.
10. Dead-lettered workflows retain enough state for explicit recovery.
11. Terminal non-recoverable workflows contain no continuation.
12. Listener failure cannot mutate workflow outcome.
13. Script code between durable boundaries is deterministic and side-effect free.

## 0.5 compatibility boundary

Durable bytes are no longer anonymous Java serialization. `JavaObjectCodec` wraps payloads in an envelope with format and codec versions plus SHA-256 integrity. The decoder supports the previous raw stream for rolling upgrades and applies a configurable class-admission policy before object construction.

Definition preflight is intentionally metadata-only for JDBC: grouped `(id, version, hash, status)` counts are compared with the compiled registry before pollers start. This catches missing deployment artifacts without attempting continuation deserialization.

Terminal retention is separated from the active scheduler tables. Archival runs in one transaction: it locks a bounded candidate set, reconstructs the full workflow and step history, writes an immutable archive row, deletes active signal/step rows, and finally deletes the active workflow row. Recoverable dead letters are not selected by the default policy.

## Engine lifecycle and scheduler isolation

One embedded engine has four execution domains:

```text
poll scheduler (1 thread)       discovers runnable workflows and due steps
handler-entry pool              enters application handlers; blocking setup is contained
callback/control scheduler      persists completions and resumes continuations
lease scheduler                 renews workflow and step fencing leases
```

The domains are deliberately separate. Handler code is entered asynchronously on the handler pool, never on the polling or callback/control schedulers. A semaphore bounds locally claimed or queued step invocations. A queued coordinated handler checks cancellation/lease loss before entering application code.

Lifecycle transitions are:

```text
RUNNING -> QUIESCING -> CLOSED
    ^          |
    +----------+  resume
```

`QUIESCING` cancels automatic polling and rejects new workflow starts. Existing drives and handlers may finish. Bounded graceful shutdown reports unfinished local work before closing; unfinished PostgreSQL work remains protected by lease expiry and fencing.

## Schema compatibility

The JDBC schema uses ordered `VNNN` resources and `durable_schema_history`. Every applied row records version, description, checksum, and installation time. Startup can ignore, warn, fail, or migrate. Historical checksum drift and a database version newer than the runtime are incompatible states.

Version 1 is the Milestone 3 baseline. Version 2 adds definition, update-order, lease-owner, and archive pagination indexes. The consolidated `postgresql-schema.sql` is documentation/bootstrap only; migration identity is defined by the immutable versioned resources.


## 0.6 production completion

### Global step concurrency

A configured step name has one row in `durable_step_concurrency_limit`. A claimant locks that row, counts live RUNNING invocations whose lease and deadline have not expired, and only then transitions another invocation to RUNNING. The limit-row lock is the serialization point across replicas. Local `maximumInFlightSteps` remains a separate JVM safety bound.

### Authenticated encryption

`AesGcmObjectCodec` wraps the existing versioned snapshot bytes rather than replacing their compatibility contract. Its envelope stores only an encryption version, key ID, random 96-bit nonce, and AES-GCM ciphertext/tag. Header fields are additional authenticated data. Non-encrypted bytes are delegated to the inner codec for rolling adoption.

### Operational aging

Stuck-work scans operate only on indexed relational columns and never deserialize CPS state. READY age, timer due time, step availability, deadline, and lease expiry are compared with database-authoritative time. Signal waits are excluded because an unbounded human/external wait is valid.

### Maintenance isolation

Retention runs on a fifth isolated execution domain: the maintenance scheduler. It does not consume poll, handler, callback, or lease-renewal threads. Active maintenance participates in graceful-drain accounting.
