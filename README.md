# Durable Groovy CPS

A small embeddable durable scripting engine built around `groovy-cps`, the continuation engine behind Jenkins Pipeline.

It compiles ordinary-looking Groovy into serializable continuations and supplies the standalone runtime around them:

- durable suspend/resume across JVM restarts
- asynchronous pluggable steps
- stable idempotency keys, retries, and execution timeouts
- durable timers and queued signals
- exact workflow source/version matching and startup preflight
- in-memory and atomic file stores for local use
- PostgreSQL persistence for multi-instance production use
- transactional workflow/step transitions
- independently claimable step rows
- isolated poll, handler-entry, callback/control, and lease-renewal executors
- bounded local step concurrency with graceful quiesce/drain shutdown
- ordered, checksummed PostgreSQL schema migrations and startup schema preflight
- workflow diagnostics with snapshot and pending-step metadata
- database-authoritative scheduling time
- renewable, fenced workflow and step leases
- dead-letter quarantine with explicit recovery controls
- searchable administration and engine health
- listener, structured logging, Micrometer, and OpenTelemetry integrations
- persisted trace propagation across suspension and restart
- deterministic crash-injection boundaries
- transactional PostgreSQL idempotency helper for database-local effects
- versioned, checksummed snapshots with legacy-read compatibility and class admission
- bounded transactional retention/archive with archived step history

The PostgreSQL shape is intentionally narrow: this is an **embedded durable workflow engine**, not a separate Temporal-style platform.

## Requirements

- JDK 17+
- Maven 3.9+
- PostgreSQL for active/active production use

The project pins Groovy 2.4.21 and `com.cloudbees:groovy-cps:1.32`. The upstream standalone CPS project is archived, so a long-term production owner should vendor or maintain that dependency.

Micrometer and OpenTelemetry are optional compile-time integrations. Applications using those listeners must include the matching library at runtime.

## Verify

```bash
./scripts/verify.sh
```

Before publishing a release, run the normal suite, PostgreSQL/Testcontainers coverage, load-tagged tests, and fixture inventory together:

```bash
./scripts/verify-release.sh
```

The PostgreSQL integration tests use Testcontainers and are skipped when no supported container runtime is available. The JDBC contract tests still run against embedded HSQLDB.

## Script

```groovy
def payment = step('charge-card', [
    orderId: input('orderId'),
    amount: input('amount')
], io.github.durablecps.api.StepOptions.retry(
    3,
    java.time.Duration.ofSeconds(2),
    java.time.Duration.ofSeconds(30)
))

def approval = awaitSignal('approve-order')
if (approval != 'approved') {
    return [status: 'rejected', payment: payment]
}

sleepMillis(250)
def shipment = step('create-shipment', [orderId: input('orderId')])
return [status: 'shipped', payment: payment, shipment: shipment]
```

The DSL supplied by `DurableScript` is deliberately small:

```groovy
input('name')
step('handler-name')
step('handler-name', serializableArgument)
step('handler-name', serializableArgument, StepOptions.retry(...))
sleepMillis(1000)
sleepFor(Duration.ofMinutes(5))
awaitSignal('signal-name')
```

## Embed with PostgreSQL

```java
PGSimpleDataSource dataSource = new PGSimpleDataSource();
dataSource.setURL("jdbc:postgresql://localhost:5432/app");
dataSource.setUser("app");
dataSource.setPassword("secret");

JdbcWorkflowStore store = new JdbcWorkflowStore(dataSource);

WorkflowDefinition orderV1 = new WorkflowDefinition(
        "order",
        1,
        Files.readString(Path.of("examples/order-workflow.groovy")));

DurableWorkflowEngine engine = DurableWorkflowEngine.builder(store)
        // Must be unique for every live process.
        .instanceId(System.getenv("HOSTNAME") + "-" + ProcessHandle.current().pid())
        .workflowLease(Duration.ofSeconds(30))
        .stepLease(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(250))
        .maximumInFlightSteps(256)
        .shutdownTimeout(Duration.ofSeconds(20))
        .schemaPreflight(SchemaPreflightMode.MIGRATE)
        .definitionPreflight(DefinitionPreflightMode.FAIL)
        .definition(orderV1)
        .step("charge-card", (request, context) -> {
            // Stable across retries, timeout takeover, and process restart.
            String idempotencyKey = request.invocationId();
            return charge(request.argument(), idempotencyKey);
        })
        .step("create-shipment", (request, context) ->
                createShipment(request.argument(), request.invocationId()))
        .listener(LoggingWorkflowListener.create())
        .build();
```

Multiple versions of the same definition may be registered together. New executions use the highest version; persisted executions require their exact version and source hash.

```java
DurableWorkflowEngine.builder(store)
        .definition(orderV1)
        .definition(orderV2)
        .build();
```

`SchemaPreflightMode.MIGRATE` applies ordered migrations before polling begins. Each migration is recorded in `durable_schema_history` with a SHA-256 checksum; changed historical SQL and databases newer than the runtime are rejected. `JdbcWorkflowStore.migrate()` remains as a compatibility alias for `migrateSchema()`.

Migration resources live under `src/main/resources/io/github/durablecps/store/migration`. Applications using Flyway or Liquibase may apply those exact files through their normal deployment process and then call `migrateSchema()` once to record/validate the engine history. `postgresql-schema.sql` is a consolidated bootstrap view, not the source of migration identity.

## Minimal production assembly

`ProductionExample.create(...)` shows the complete intended deployment shape: PostgreSQL migrations, exact-definition preflight, AES-GCM snapshot encryption, global step concurrency, bounded local dispatch, scheduled retention, and graceful shutdown. The application still supplies its own `DataSource`, key provider, and step handlers.

## Database-global step concurrency

Local `maximumInFlightSteps` protects one JVM. A named limit protects the external dependency across every replica:

```java
.stepConcurrencyLimit("charge-card", 32)
.stepConcurrencyLimit("create-shipment", 64)
```

The first replica persists the limit in `durable_step_concurrency_limit`. Later replicas must register the same value or startup fails. Claims for the same step name serialize through the limit row, and only live `RUNNING` leases count against capacity.

## Encrypted snapshots and key rotation

Wrap the compatible Java snapshot codec with `AesGcmObjectCodec`:

```java
EncryptionKey current = new EncryptionKey("2026-08", currentAesKey);
EncryptionKey previous = new EncryptionKey("2026-01", previousAesKey);
AesGcmObjectCodec codec = new AesGcmObjectCodec(
        new JavaObjectCodec(policy),
        new MapEncryptionKeyProvider(current.id(), List.of(previous, current)));

JdbcWorkflowStore store = new JdbcWorkflowStore(dataSource, codec);
DurableWorkflowEngine engine = DurableWorkflowEngine.builder(store)
        .codec(codec)
        // ...
        .build();
```

New writes use the active key. Historical key IDs remain readable during rotation. The AES-GCM envelope authenticates the key ID, nonce, and ciphertext. Readers also accept older unencrypted snapshots, permitting a rolling encryption rollout after all readers have been upgraded. Key material is never stored by the engine.

## Stuck-work inspection and CLI

```java
StuckWorkReport report = engine.administration()
        .stuckWork(Duration.ofMinutes(5), 100);
```

Signal waits are intentionally excluded. The report finds READY workflows not driven by the threshold, overdue timers, claimable steps left pending, and RUNNING steps whose deadline or lease has remained expired.

A small PostgreSQL CLI is included:

```bash
export DURABLE_CPS_JDBC_URL='jdbc:postgresql://db/workflows'
export DURABLE_CPS_JDBC_USER='workflow_admin'
export DURABLE_CPS_JDBC_PASSWORD='...'

./scripts/admin.sh health
./scripts/admin.sh schema
./scripts/admin.sh limits
./scripts/admin.sh stuck 300 100
./scripts/admin.sh archive 2592000 1000
```

The CLI does not start workflow execution or accept definition code.

## Lifecycle and execution isolation

The poller, handler-entry pool, callback/control scheduler, and lease-renewal scheduler are separate. A handler whose `execute` method blocks cannot stop durable polling or fencing renewal. `maximumInFlightSteps` bounds local claimed/queued handlers; excess due work remains durable and claimable on a later tick.

```java
engine.quiesce();                 // stop polling and reject new starts
engine.resume();                  // restore polling/admission
ShutdownResult result = engine.closeGracefully(Duration.ofSeconds(20));
```

Quiescing still allows already claimed handlers and their continuation drives to finish. If the shutdown deadline expires, the engine closes and reports the remaining active polls, drivers, and steps; PostgreSQL lease expiry plus fencing enables another instance to recover them.

## Operational administration

The engine exposes Java operations rather than embedding an HTTP server:

```java
WorkflowAdministration admin = engine.administration();

WorkflowPage failures = admin.search(
        WorkflowQuery.all()
                .withStatuses(Set.of(WorkflowStatus.DEAD_LETTERED))
                .withDefinition("order")
                .withPage(100, 0));

EngineHealth health = admin.health();
WorkflowDiagnostic diagnostic = admin.diagnose(workflowId);
Optional<SchemaStatus> schema = admin.schemaStatus();

admin.quiesce();
admin.resume();
admin.retry(workflowId, "dependency restored");
admin.resumeWithValue(workflowId, replacementResult, "verified externally");
admin.resumeWithFailure(workflowId, failure, "force script catch path");
admin.deadLetter(workflowId, "operator quarantine");
admin.cancel(workflowId, "request withdrawn");
```

Applications can wrap this API with Spring MVC, JAX-RS, a CLI, or an internal console. Every mutating operation appends a workflow history entry.

### Failure classification

- `FAILED`: the Groovy workflow itself completed by throwing an unhandled exception. It is terminal.
- `DEAD_LETTERED`: execution cannot safely continue automatically, but the continuation is retained for explicit recovery.
- `CANCELLED`: the workflow was intentionally stopped.

Examples that dead-letter:

- a step exhausts retries
- the exact workflow definition is missing
- continuation decoding fails
- the CPS runtime suspends with an unsupported value
- infrastructure fails while resuming the continuation

A dead-lettered pending PostgreSQL step is cancelled transactionally so a poller cannot continue dispatching it behind the operator's back. `retry` requeues the same durable invocation and preserves its invocation ID.

## Observability

### Listener SPI

```java
engine = DurableWorkflowEngine.builder(store)
        .listener(new WorkflowListener() {
            @Override
            public void onWorkflow(WorkflowEvent event) {
                audit(event);
            }

            @Override
            public void onStep(StepEvent event) {
                audit(event);
            }
        })
        .build();
```

Listener failures do not fail workflows. They are reported to the remaining listeners as `EngineEventType.LISTENER_FAILURE`.

### Micrometer

```java
MeterRegistry registry = ...;

engine = DurableWorkflowEngine.builder(store)
        .listener(new MicrometerWorkflowListener(registry))
        .build();
```

Emitted meter families:

```text
durable.workflow.events
durable.workflow.duration
durable.step.events
durable.step.duration
durable.engine.events
```

Tags are intentionally low-cardinality: definition, status, step, event, and outcome. Workflow and invocation IDs are never metric tags.

### OpenTelemetry

One object acts as both listener and durable context propagator:

```java
OpenTelemetryWorkflowObservability telemetry =
        new OpenTelemetryWorkflowObservability(openTelemetry);

engine = DurableWorkflowEngine.builder(store)
        .contextPropagator(telemetry)
        .listener(telemetry)
        .build();
```

The current text-map propagation context is captured when the workflow starts, persisted in `WorkflowRecord`, restored around CPS execution and step-handler invocation, and used as the parent for lifecycle spans after restart.

## PostgreSQL storage shape

`durable_workflow` contains the opaque CPS snapshot plus indexed state used by the poller.

`durable_step_invocation` contains independently claimable steps with:

- invocation ID and durable sequence
- lifecycle: `PENDING`, `RUNNING`, `RETRY_WAIT`, `SUCCEEDED`, `FAILED`, `CANCELLED`
- attempt and retry schedule
- execution deadline
- lease owner, expiry, and monotonically increasing fencing token
- durable result or failure state

`durable_signal` contains queued signals independently of the continuation blob.

`durable_idempotency` caches results for handlers that perform their business mutation through the same PostgreSQL connection.

The important transactions are:

```text
suspend transaction:
  update workflow to WAITING with continuation
  + insert step invocation
  + commit

completion transaction:
  verify live step fencing token
  + store step result/retry/dead-letter failure
  + move workflow to READY, keep WAITING, or quarantine as DEAD_LETTERED
  + commit

administrative recovery transaction:
  replace dead-lettered workflow state
  + requeue/complete/fail/cancel its pending step row
  + commit
```

A stale process cannot commit after lease takeover because every leased write checks the owner, token, and expiry in SQL.

## Delivery and timeout semantics

Steps are **at least once**. Every invocation receives the stable key:

```text
<workflow-id>:<durable-call-sequence>
```

A handler may be invoked again after:

- process death
- lease expiry
- execution timeout
- failure after an external side effect but before result persistence
- explicit administrative retry

Use `request.invocationId()` to deduplicate externally visible effects. The business side effect and idempotency record should share one business transaction where possible.

For effects in the workflow PostgreSQL database, the included helper does exactly that:

```java
JdbcIdempotencyStore once = new JdbcIdempotencyStore(dataSource);

return CompletableFuture.completedFuture(once.executeOnce(
        request.invocationId(),
        String.class,
        connection -> {
            insertPayment(connection, request.argument());
            return "payment-receipt";
        }));
```

The reservation row, callback writes, and serialized result commit together. It cannot make HTTP, Kafka, or another database exactly once; those systems still need their own idempotency support.

`StepOptions.executionTimeout` is a durable deadline. It is not merely `Future.cancel(true)`: after the deadline, lease renewal and completion are rejected, and another node may claim the same invocation with a higher fencing token.

## Script discipline

Code between `step`, `sleepFor`, and `awaitSignal` boundaries may be replayed. It must be deterministic and free of external side effects.

Good:

```groovy
def total = input('quantity') * input('unitPrice')
def receipt = step('charge', [total: total])
```

Wrong:

```groovy
database.insert(...) // external mutation outside a durable step
def receipt = step('charge', ...)
```

All values reachable from a suspension point must be serializable. Do not retain files, sockets, threads, executors, framework contexts, or live database objects in script locals.

## Security boundary

Workflow source and persistence are trusted. The engine executes Groovy and uses Java serialization inside the durable snapshot envelope. `AesGcmObjectCodec` provides authenticated encryption at rest, but neither encryption nor class allowlisting makes hostile workflow source or hostile serialized input safe. Do not expose arbitrary script upload or snapshot data to untrusted users.

## Deliberately deferred

- a transport-specific administrative HTTP module (the JDBC CLI is included)
- remote worker transport
- child workflows and parallel branches
- saga/compensation DSL
- continuation/source migration
- sandboxed untrusted Groovy
- replacement of the Java-serialization payload with a language-neutral structured continuation format
- exactly-once claims for external systems

See `ARCHITECTURE.md` and `OPERATIONS.md` for the state, crash, and recovery protocols.

## Snapshot compatibility and admission policy

Version 0.6 continues to write every Java-serialization payload inside a self-describing durable snapshot envelope containing:

- envelope version
- codec identifier and codec version
- payload length
- SHA-256 payload checksum

`JavaObjectCodec` continues to read the legacy raw Java-serialization stream written by 0.3, enabling a rolling upgrade. The repository contains a checked-in 0.3 compatibility fixture under `src/test/resources/compatibility/0.3.0`.

The default decoder admits only JDK, Groovy/CPS, engine, and generated `Durable_*` classes. Dynamic proxies are rejected. Applications using custom serializable inputs, outputs, or step arguments must configure and share one codec between the engine and store:

```java
DeserializationPolicy policy = DeserializationPolicy.builder()
        .allowPackage("java.")
        .allowPackage("groovy.")
        .allowPackage("org.codehaus.groovy.")
        .allowPackage("com.cloudbees.groovy.cps.")
        .allowPackage("io.github.durablecps.")
        .allowPackage("com.mycompany.orders.")
        .allowGeneratedWorkflowClasses(true)
        .maximumBytes(32 * 1024 * 1024)
        .build();

JavaObjectCodec codec = new JavaObjectCodec(policy);
JdbcWorkflowStore store = new JdbcWorkflowStore(dataSource, codec);
DurableWorkflowEngine engine = DurableWorkflowEngine.builder(store)
        .codec(codec)
        .definition(orderV1)
        .build();
```

This remains a trusted-store format. Allowlisting, checksums, depth/reference/array limits, and proxy rejection are hardening controls—not a claim that Java serialization is safe for hostile input.

## Definition preflight

Persisted `READY`, `WAITING`, and `DEAD_LETTERED` workflows require their exact definition ID, version, and source hash. Preflight runs before polling starts:

```java
DurableWorkflowEngine engine = DurableWorkflowEngine.builder(store)
        .definition(orderV1)
        .definition(orderV2)
        .definitionPreflight(DefinitionPreflightMode.FAIL)
        .build();
```

Modes are `OFF`, `WARN` (default), and `FAIL`. Operators can inspect the same report at runtime:

```java
DefinitionVerificationReport report = engine.administration().verifyDefinitions();
```

The JDBC report is computed from indexed metadata and does not deserialize CPS snapshots.

## Retention and archival

JDBC archival atomically copies each selected terminal workflow and all of its step invocation records to `durable_workflow_archive`, then removes its active workflow, signal, and step rows.

```java
ArchiveResult result = engine.administration().archive(
        ArchiveRequest.completedBefore(Instant.now().minus(Duration.ofDays(30)))
                .withLimit(1_000));

ArchivedWorkflow archived = engine.administration()
        .archived(result.archived().get(0).archiveId())
        .orElseThrow();
```

The default request archives `COMPLETED`, `FAILED`, and `CANCELLED`. `DEAD_LETTERED` is deliberately excluded because it is recoverable, but may be included explicitly. Workflow history is bounded to 1,000 entries, preserving the origin event and newest 999 events.

Periodic retention can be embedded directly:

```java
.retentionPolicy(RetentionPolicy.standard(Duration.ofDays(30)))
```

Archival runs on a dedicated maintenance scheduler, stops while the engine is quiesced, participates in graceful drain, and processes one bounded batch per interval.

## Capacity smoke

The normal suite excludes the opt-in load tag. Run the 100,000-waiting-workflow in-memory capacity smoke with:

```bash
./scripts/verify-load.sh
```

## License

This project is MIT licensed. Its `groovy-cps` dependency is Apache-2.0 licensed.
