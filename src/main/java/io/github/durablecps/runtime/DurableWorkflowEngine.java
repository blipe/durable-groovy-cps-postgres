package io.github.durablecps.runtime;

import com.cloudbees.groovy.cps.Continuable;
import com.cloudbees.groovy.cps.Outcome;
import io.github.durablecps.admin.ArchiveRequest;
import io.github.durablecps.admin.ArchiveResult;
import io.github.durablecps.admin.ArchivedWorkflow;
import io.github.durablecps.admin.ArchivedWorkflowSummary;
import io.github.durablecps.admin.DefinitionCompatibilityIssue;
import io.github.durablecps.admin.DefinitionPreflightException;
import io.github.durablecps.admin.DefinitionPreflightMode;
import io.github.durablecps.admin.DefinitionVerificationReport;
import io.github.durablecps.admin.EngineHealth;
import io.github.durablecps.admin.EngineLifecycleState;
import io.github.durablecps.admin.SchemaPreflightException;
import io.github.durablecps.admin.SchemaPreflightMode;
import io.github.durablecps.admin.RetentionPolicy;
import io.github.durablecps.admin.StuckWorkReport;
import io.github.durablecps.admin.ShutdownResult;
import io.github.durablecps.admin.WorkflowDiagnostic;
import io.github.durablecps.admin.HealthStatus;
import io.github.durablecps.admin.WorkflowAdministration;
import io.github.durablecps.admin.WorkflowPage;
import io.github.durablecps.admin.WorkflowQuery;
import io.github.durablecps.api.ConcurrentWorkflowUpdateException;
import io.github.durablecps.api.DefinitionMismatchException;
import io.github.durablecps.api.DurableCommand;
import io.github.durablecps.api.DurableStepException;
import io.github.durablecps.api.StepContext;
import io.github.durablecps.api.StepHandler;
import io.github.durablecps.api.StepRequest;
import io.github.durablecps.api.WorkflowDefinition;
import io.github.durablecps.api.WorkflowStatus;
import io.github.durablecps.api.WorkflowView;
import io.github.durablecps.runtime.state.FailureData;
import io.github.durablecps.runtime.state.HistoryEntry;
import io.github.durablecps.runtime.state.ResumeValue;
import io.github.durablecps.runtime.state.SignalEnvelope;
import io.github.durablecps.runtime.state.WorkflowRecord;
import io.github.durablecps.observability.EngineEvent;
import io.github.durablecps.observability.EngineEventType;
import io.github.durablecps.observability.StepEvent;
import io.github.durablecps.observability.StepEventType;
import io.github.durablecps.observability.WorkflowContextPropagator;
import io.github.durablecps.observability.WorkflowEvent;
import io.github.durablecps.observability.WorkflowEventType;
import io.github.durablecps.observability.WorkflowListener;
import io.github.durablecps.store.ArchiveCapableWorkflowStore;
import io.github.durablecps.store.ClaimedStep;
import io.github.durablecps.store.CoordinatedWorkflowStore;
import io.github.durablecps.store.DefinitionInventoryStore;
import io.github.durablecps.store.LeaseLostException;
import io.github.durablecps.store.StepInvocationRecord;
import io.github.durablecps.store.StoredDefinitionReference;
import io.github.durablecps.store.SearchableWorkflowStore;
import io.github.durablecps.store.SchemaManagedWorkflowStore;
import io.github.durablecps.store.SchemaStatus;
import io.github.durablecps.store.StepLease;
import io.github.durablecps.store.StepConcurrencyStore;
import io.github.durablecps.store.StuckWorkStore;
import io.github.durablecps.store.WorkflowLease;
import io.github.durablecps.store.WorkflowRecordPage;
import io.github.durablecps.store.WorkflowStore;
import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Small embeddable durable CPS runtime. A {@link CoordinatedWorkflowStore} enables active/active nodes.
 *
 * <p>Script code must be deterministic between durable calls. All external effects belong in registered
 * {@link StepHandler}s, which are at-least-once and receive a stable invocation id for deduplication.</p>
 */
public final class DurableWorkflowEngine implements AutoCloseable {
    private static final int CAS_RETRIES = 32;

    private final WorkflowStore store;
    private final CoordinatedWorkflowStore coordinatedStore;
    private final ObjectCodec codec;
    private final WorkflowRegistry workflows;
    private final StepRegistry steps;
    private final Clock clock;
    /** Callback/control scheduler exposed through StepContext. */
    private final ScheduledExecutorService executor;
    private final boolean ownsExecutor;
    /** Dedicated handler-entry executor so blocking handler setup cannot starve callbacks. */
    private final ExecutorService handlerExecutor;
    private final boolean ownsHandlerExecutor;
    /** Dedicated polling scheduler so blocking handlers cannot stall durable discovery. */
    private final ScheduledExecutorService pollExecutor;
    private final boolean ownsPollExecutor;
    /** Dedicated lease-renewal scheduler so blocking handlers cannot lose fencing renewal. */
    private final ScheduledExecutorService leaseExecutor;
    private final boolean ownsLeaseExecutor;
    /** Dedicated maintenance scheduler so archival never blocks polling or workflow callbacks. */
    private final ScheduledExecutorService maintenanceExecutor;
    private final boolean ownsMaintenanceExecutor;
    private final Duration pollInterval;
    private final String instanceId;
    private final Duration workflowLeaseDuration;
    private final Duration stepLeaseDuration;
    private final int claimBatchSize;
    private final int maximumInFlightSteps;
    private final boolean automaticPolling;
    private final Duration shutdownTimeout;
    private final DefinitionPreflightMode definitionPreflightMode;
    private final SchemaPreflightMode schemaPreflightMode;
    private final RetentionPolicy retentionPolicy;
    private final Map<String, Integer> stepConcurrencyLimits;
    private final FailureInjector failureInjector;
    private final WorkflowContextPropagator contextPropagator;
    private final List<WorkflowListener> listeners;
    private final WorkflowAdministration administration = new Administration();
    private final ConcurrentHashMap<String, Boolean> driving = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> inFlight = new ConcurrentHashMap<>();
    private final Semaphore stepPermits;
    private final AtomicInteger activeStarts = new AtomicInteger();
    private final AtomicInteger activePolls = new AtomicInteger();
    private final AtomicInteger activeMaintenance = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<EngineLifecycleState> lifecycle =
            new AtomicReference<>(EngineLifecycleState.RUNNING);
    private final Object pollLifecycleLock = new Object();
    private final Object maintenanceLifecycleLock = new Object();
    private volatile ScheduledFuture<?> pollTask;
    private volatile ScheduledFuture<?> maintenanceTask;

    private DurableWorkflowEngine(Builder builder) {
        this.store = builder.store;
        this.coordinatedStore = builder.store instanceof CoordinatedWorkflowStore coordinated ? coordinated : null;
        this.codec = builder.codec;
        this.clock = builder.clock;
        this.executor = builder.executor != null
                ? builder.executor
                : newDaemonScheduler("durable-cps-control", 2);
        this.ownsExecutor = builder.executor == null;
        this.handlerExecutor = builder.handlerExecutor != null
                ? builder.handlerExecutor
                : newDaemonExecutor(
                        "durable-cps-handler",
                        Math.min(maximumHandlerThreads(builder.maximumInFlightSteps),
                                builder.maximumInFlightSteps));
        this.ownsHandlerExecutor = builder.handlerExecutor == null;
        this.pollExecutor = builder.pollExecutor != null
                ? builder.pollExecutor
                : newDaemonScheduler("durable-cps-poll", 1);
        this.ownsPollExecutor = builder.pollExecutor == null;
        this.leaseExecutor = builder.leaseExecutor != null
                ? builder.leaseExecutor
                : newDaemonScheduler("durable-cps-lease", 2);
        this.ownsLeaseExecutor = builder.leaseExecutor == null;
        this.maintenanceExecutor = builder.maintenanceExecutor != null
                ? builder.maintenanceExecutor
                : newDaemonScheduler("durable-cps-maintenance", 1);
        this.ownsMaintenanceExecutor = builder.maintenanceExecutor == null;
        this.pollInterval = builder.pollInterval;
        this.instanceId = builder.instanceId;
        this.workflowLeaseDuration = builder.workflowLeaseDuration;
        this.stepLeaseDuration = builder.stepLeaseDuration;
        this.claimBatchSize = builder.claimBatchSize;
        this.maximumInFlightSteps = builder.maximumInFlightSteps;
        this.stepPermits = new Semaphore(maximumInFlightSteps);
        this.automaticPolling = builder.automaticPolling;
        this.shutdownTimeout = builder.shutdownTimeout;
        this.definitionPreflightMode = builder.definitionPreflightMode;
        this.schemaPreflightMode = builder.schemaPreflightMode;
        this.retentionPolicy = builder.retentionPolicy;
        this.stepConcurrencyLimits = Map.copyOf(builder.stepConcurrencyLimits);
        this.failureInjector = builder.failureInjector;
        this.contextPropagator = builder.contextPropagator;
        this.listeners = List.copyOf(builder.listeners);
        this.workflows = new WorkflowRegistry(builder.parentClassLoader);
        this.steps = new StepRegistry();
        try {
            builder.definitions.forEach(workflows::register);
            builder.handlers.forEach(steps::register);
            runSchemaPreflight();
            validateRetentionConfiguration();
            registerStepConcurrencyLimits();
            runDefinitionPreflight();
            if (automaticPolling) schedulePoller();
            if (retentionPolicy != null) scheduleMaintenance();
        } catch (RuntimeException | Error failure) {
            cleanupFailedConstruction(failure);
            throw failure;
        }
    }

    public static Builder builder(WorkflowStore store) {
        return new Builder(store);
    }

    public WorkflowAdministration administration() {
        ensureOpen();
        return administration;
    }

    public EngineLifecycleState lifecycle() {
        return lifecycle.get();
    }

    /** Stop polling and reject new workflow starts while allowing active handlers and drives to finish. */
    public void quiesce() {
        ensureOpen();
        if (lifecycle.compareAndSet(EngineLifecycleState.RUNNING, EngineLifecycleState.QUIESCING)) {
            cancelPoller();
            cancelMaintenance();
            emitEngine(EngineEventType.ENGINE_QUIESCING, null, null, "engine is quiescing", null);
        }
    }

    /** Resume polling and workflow admission after a quiesce operation. */
    public void resume() {
        ensureOpen();
        if (lifecycle.compareAndSet(EngineLifecycleState.QUIESCING, EngineLifecycleState.RUNNING)) {
            if (automaticPolling) schedulePoller();
            if (retentionPolicy != null) scheduleMaintenance();
            emitEngine(EngineEventType.ENGINE_RESUMED, null, null, "engine resumed", null);
        }
    }

    /**
     * Stop admission, wait for active polls/drives/handlers, then close all owned resources.
     * A timeout is safe: persisted leases and fencing permit another instance to recover the work.
     */
    public ShutdownResult closeGracefully(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) throw new IllegalArgumentException("timeout must not be negative");
        Instant started = clock.instant();
        if (closed.get()) {
            return new ShutdownResult(true, 0, 0, 0, 0, 0, Duration.ZERO, clock.instant());
        }
        quiesce();
        long deadline = System.nanoTime() + timeout.toNanos();
        boolean drained;
        do {
            drained = activeStarts.get() == 0 && activePolls.get() == 0
                    && activeMaintenance.get() == 0
                    && driving.isEmpty() && inFlight.isEmpty();
            if (drained || System.nanoTime() >= deadline) break;
            try {
                Thread.sleep(10L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
        } while (true);
        int starts = activeStarts.get();
        int polls = activePolls.get();
        int drivers = driving.size();
        int steps = inFlight.size();
        int maintenance = activeMaintenance.get();
        forceClose();
        Instant completed = clock.instant();
        ShutdownResult result = new ShutdownResult(
                drained, starts, polls, drivers, steps, maintenance,
                nonNegativeDuration(started, completed), completed);
        return result;
    }

    public void registerDefinition(WorkflowDefinition definition) {
        ensureOpen();
        workflows.register(definition);
    }

    public void registerStep(String name, StepHandler handler) {
        ensureOpen();
        steps.register(name, handler);
    }

    public String start(String definitionId, Map<String, ? extends Serializable> input) {
        return start(UUID.randomUUID().toString(), definitionId, input);
    }

    public String start(String workflowId, String definitionId, Map<String, ? extends Serializable> input) {
        beginStart();
        try {
            validateWorkflowId(workflowId);
            CompiledWorkflow definition = workflows.requireLatest(definitionId);
            Continuable continuable = definition.newContinuable(input == null ? Map.of() : input);
            byte[] snapshot = codec.encode(continuable);
            Instant now = currentTime();
            WorkflowRecord initial = new WorkflowRecord(
                    workflowId,
                    0L,
                    definitionId,
                    definition.definition().version(),
                    definition.hash(),
                    WorkflowStatus.READY,
                    snapshot,
                    null,
                    new ResumeValue.Success(null),
                    null,
                    null,
                    0,
                    now,
                    now,
                    now,
                    contextPropagator.capture(),
                    List.of(),
                    List.of(new HistoryEntry(
                            now, "STARTED", definitionId + "@" + definition.definition().version())));
            WorkflowRecord created = store.create(initial);
            emitWorkflow(null, created);
            drive(workflowId);
            return workflowId;
        } finally {
            activeStarts.decrementAndGet();
        }
    }

    public Optional<WorkflowView> get(String workflowId) {
        ensureOpen();
        return store.load(workflowId).map(this::view);
    }

    public List<WorkflowView> list() {
        ensureOpen();
        return store.list().stream().map(this::view).toList();
    }

    public void signal(String workflowId, String signalName, Serializable payload) {
        ensureOpen();
        Objects.requireNonNull(signalName, "signalName");
        for (int retry = 0; retry < CAS_RETRIES; retry++) {
            WorkflowRecord current = requireRecord(workflowId);
            if (current.status().terminal()) {
                throw new IllegalStateException("workflow is terminal: " + workflowId + " is " + current.status());
            }
            Instant now = currentTime();
            WorkflowRecord replacement;
            if (current.status() == WorkflowStatus.WAITING
                    && current.pendingCommand() instanceof DurableCommand.AwaitSignal awaiting
                    && awaiting.name().equals(signalName)) {
                replacement = current.next(
                        WorkflowStatus.READY,
                        current.continuation(),
                        null,
                        new ResumeValue.Success(payload),
                        null,
                        null,
                        0,
                        now,
                        current.signals(),
                        new HistoryEntry(now, "SIGNAL_DELIVERED", signalName),
                        now);
            } else {
                replacement = current.withSignal(new SignalEnvelope(signalName, payload, now), now);
            }
            try {
                WorkflowRecord stored = store.replace(workflowId, current.revision(), replacement);
                emitWorkflow(current, stored);
                if (stored.status() == WorkflowStatus.READY) drive(workflowId);
                return;
            } catch (ConcurrentWorkflowUpdateException ignored) {
                // Retry against the newest state.
            }
        }
        throw new IllegalStateException("could not deliver signal after concurrent updates: " + workflowId);
    }

    public void cancel(String workflowId, String reason) {
        ensureOpen();
        String detail = reason == null || reason.isBlank() ? "cancelled" : reason;
        for (int retry = 0; retry < CAS_RETRIES; retry++) {
            WorkflowRecord current = requireRecord(workflowId);
            if (current.status() == WorkflowStatus.CANCELLED) return;
            if (current.status() == WorkflowStatus.COMPLETED
                    || current.status() == WorkflowStatus.FAILED) {
                throw new IllegalStateException(
                        "workflow is terminal: " + workflowId + " is " + current.status());
            }
            Instant now = currentTime();
            String pendingInvocationId = current.pendingCommand() instanceof DurableCommand.Step step
                    ? invocationId(workflowId, step.sequence())
                    : null;
            WorkflowRecord replacement = current.next(
                    WorkflowStatus.CANCELLED,
                    null,
                    null,
                    null,
                    null,
                    null,
                    current.attempt(),
                    null,
                    current.signals(),
                    new HistoryEntry(now, "CANCELLED", detail),
                    now);
            try {
                WorkflowRecord stored = coordinatedStore == null
                        ? store.replace(workflowId, current.revision(), replacement)
                        : coordinatedStore.cancelWorkflow(
                                current.revision(), replacement, pendingInvocationId);
                emitWorkflow(current, stored);
                if (pendingInvocationId != null) {
                    DurableCommand.Step step = (DurableCommand.Step) current.pendingCommand();
                    emitStep(
                            StepEventType.CANCELLED,
                            stored,
                            pendingInvocationId,
                            step.name(),
                            current.attempt(),
                            step.options().maxAttempts(),
                            null,
                            null,
                            now,
                            detail);
                }
                return;
            } catch (ConcurrentWorkflowUpdateException ignored) {
                // Retry against the newest durable state.
            }
        }
        throw new IllegalStateException(
                "could not cancel workflow after concurrent changes: " + workflowId);
    }

    /** Poll runnable workflows and independently claimable step invocations once. */
    public void tick() {
        ensureOpen();
        if (lifecycle.get() != EngineLifecycleState.RUNNING) return;
        activePolls.incrementAndGet();
        try {
            if (lifecycle.get() == EngineLifecycleState.RUNNING) tickOnce();
        } finally {
            activePolls.decrementAndGet();
        }
    }

    private void tickOnce() {
        if (coordinatedStore != null) {
            for (String workflowId : coordinatedStore.findRunnableWorkflowIds(claimBatchSize)) {
                try {
                    WorkflowRecord record = requireRecord(workflowId);
                    if (record.status() == WorkflowStatus.READY) {
                        drive(workflowId);
                    } else if (record.status() == WorkflowStatus.WAITING
                            && record.pendingCommand() instanceof DurableCommand.Sleep) {
                        awakenTimer(workflowId);
                    } else if (record.status() == WorkflowStatus.WAITING
                            && record.pendingCommand() instanceof DurableCommand.AwaitSignal) {
                        deliverQueuedSignal(workflowId);
                    }
                } catch (RuntimeException failure) {
                    emitEngine(EngineEventType.POLL_FAILURE, workflowId, null, "workflow polling failed: "+workflowId, failure);
                }
            }
            for (String invocationId : coordinatedStore.findDueStepInvocationIds(claimBatchSize)) {
                try {
                    dispatchCoordinatedStep(invocationId);
                } catch (RuntimeException failure) {
                    emitEngine(EngineEventType.POLL_FAILURE, null, invocationId, "step polling failed: "+invocationId, failure);
                }
            }
            return;
        }

        Collection<WorkflowRecord> records = store.list();
        Instant now = currentTime();
        for (WorkflowRecord record : records) {
            try {
                if (record.status() == WorkflowStatus.READY) {
                    drive(record.id());
                } else if (record.status() == WorkflowStatus.WAITING) {
                    DurableCommand command = record.pendingCommand();
                    if (command instanceof DurableCommand.Step && isDue(record, now)) {
                        dispatchStep(record.id());
                    } else if (command instanceof DurableCommand.Sleep && isDue(record, now)) {
                        awakenTimer(record.id());
                    } else if (command instanceof DurableCommand.AwaitSignal) {
                        deliverQueuedSignal(record.id());
                    }
                }
            } catch (RuntimeException failure) {
                emitEngine(EngineEventType.POLL_FAILURE, record.id(), null, "workflow polling failed: "+record.id(), failure);
            }
        }
    }

    /** Useful for tests and command-line embedding; waiting timers/signals count as idle. */
    public boolean runUntilIdle(Duration timeout) {
        ensureOpen();
        Instant deadline = currentTime().plus(timeout);
        long wallDeadline = System.nanoTime() + timeout.toNanos();
        do {
            tick();
            boolean runnable;
            if (coordinatedStore != null) {
                runnable = !coordinatedStore.findRunnableWorkflowIds(1).isEmpty()
                        || !coordinatedStore.findDueStepInvocationIds(1).isEmpty();
            } else {
                Instant now = currentTime();
                runnable = store.list().stream().anyMatch(record -> isRunnableNow(record, now));
            }
            if (!runnable && driving.isEmpty() && inFlight.isEmpty()) return true;
            try {
                Thread.sleep(5L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        } while (System.nanoTime() < wallDeadline && currentTime().isBefore(deadline));
        return false;
    }

    private void drive(String workflowId) {
        if (driving.putIfAbsent(workflowId, Boolean.TRUE) != null) return;
        WorkflowLease workflowLease = null;
        LeaseGuard leaseGuard = null;
        try {
            if (coordinatedStore != null) {
                Optional<WorkflowLease> acquired = coordinatedStore.tryAcquireWorkflowLease(
                        workflowId, instanceId, workflowLeaseDuration);
                if (acquired.isEmpty()) return;
                workflowLease = acquired.get();
                failureInjector.hit(Failpoint.AFTER_WORKFLOW_LEASE_ACQUIRED, workflowId, null);
                WorkflowLease heldLease = workflowLease;
                leaseGuard = startLeaseGuard(
                        workflowLeaseDuration,
                        () -> coordinatedStore.renewWorkflowLease(heldLease, workflowLeaseDuration));
            }

            for (int transitions = 0; transitions < 10_000; transitions++) {
                if (leaseGuard != null && leaseGuard.lost()) return;
                WorkflowRecord current = requireRecord(workflowId);
                if (current.status() != WorkflowStatus.READY) return;
                CompiledWorkflow definition;
                Continuable continuation;
                Outcome resume;
                try {
                    definition = verifyDefinition(current);
                    continuation = codec.decode(
                            current.continuation(), Continuable.class, definition.classLoader());
                    resume = toOutcome(current.resumeValue());
                } catch (Throwable failure) {
                    persistEngineFailure(current, failure, workflowLease);
                    return;
                }
                failureInjector.hit(Failpoint.BEFORE_CONTINUATION_RESUME, workflowId, null);
                Outcome yielded;
                try (WorkflowContextPropagator.Scope ignored =
                        contextPropagator.restore(current.telemetryContext())) {
                    yielded = runWithCpsCategories(continuation, resume);
                } catch (Throwable failure) {
                    persistEngineFailure(current, failure, workflowLease);
                    return;
                }
                failureInjector.hit(Failpoint.AFTER_CONTINUATION_RESUME, workflowId, null);

                Instant now = currentTime();
                WorkflowRecord replacement;
                if (yielded.isFailure()) {
                    Throwable failure = yielded.getAbnormal();
                    replacement = current.next(
                            WorkflowStatus.FAILED,
                            null,
                            null,
                            null,
                            null,
                            FailureData.from(failure),
                            current.attempt(),
                            null,
                            current.signals(),
                            new HistoryEntry(now, "FAILED", failure.toString()),
                            now);
                } else if (!continuation.isResumable()) {
                    Serializable output = DurableScript.requireSerializable(yielded.getNormal(), "workflow output");
                    if (output != null) codec.encode(output);
                    replacement = current.next(
                            WorkflowStatus.COMPLETED,
                            null,
                            null,
                            null,
                            output,
                            null,
                            current.attempt(),
                            null,
                            current.signals(),
                            new HistoryEntry(now, "COMPLETED", output == null ? "null" : output.getClass().getName()),
                            now);
                } else {
                    Object suspended = yielded.getNormal();
                    if (!(suspended instanceof DurableCommand command)) {
                        persistEngineFailure(current, new IllegalStateException(
                                "CPS script suspended with unsupported value: " + suspended), workflowLease);
                        return;
                    }
                    byte[] nextSnapshot = codec.encode(continuation);
                    failureInjector.hit(
                            Failpoint.AFTER_CONTINUATION_SERIALIZED,
                            workflowId,
                            command instanceof DurableCommand.Step step
                                    ? invocationId(workflowId, step.sequence())
                                    : null);
                    replacement = blockedRecord(current, nextSnapshot, command, now);
                }

                try {
                    WorkflowRecord stored;
                    if (coordinatedStore != null) {
                        if (workflowLease == null) throw new IllegalStateException("missing workflow lease");
                        if (replacement.status() == WorkflowStatus.WAITING
                                && replacement.pendingCommand() instanceof DurableCommand.Step step) {
                            String invocationId = invocationId(workflowId, step.sequence());
                            StepInvocationRecord pending = StepInvocationRecord.pending(
                                    invocationId,
                                    workflowId,
                                    step.sequence(),
                                    step.name(),
                                    step.argument(),
                                    step.options(),
                                    now);
                            stored = coordinatedStore.suspendWithStep(
                                    workflowLease, current.revision(), replacement, pending);
                        } else {
                            stored = coordinatedStore.replaceWithLease(
                                    workflowLease, current.revision(), replacement);
                        }
                    } else {
                        stored = store.replace(workflowId, current.revision(), replacement);
                    }

                    emitWorkflow(current, stored);
                    if (stored.status() == WorkflowStatus.WAITING) {
                        failureInjector.hit(
                                Failpoint.AFTER_WORKFLOW_SUSPENDED,
                                workflowId,
                                stored.pendingCommand() instanceof DurableCommand.Step step
                                        ? invocationId(workflowId, step.sequence())
                                        : null);
                    }
                    if (stored.status() != WorkflowStatus.READY) {
                        if (stored.status() == WorkflowStatus.WAITING
                                && stored.pendingCommand() instanceof DurableCommand.Step step
                                && isDue(stored, currentTime())) {
                            if (coordinatedStore != null) {
                                dispatchCoordinatedStep(invocationId(workflowId, step.sequence()));
                            } else {
                                dispatchStep(workflowId);
                            }
                        } else if (stored.status() == WorkflowStatus.WAITING
                                && stored.pendingCommand() instanceof DurableCommand.Sleep
                                && isDue(stored, currentTime())) {
                            awakenTimer(workflowId);
                        }
                        return;
                    }
                } catch (ConcurrentWorkflowUpdateException ignored) {
                    // Re-run from the last persisted continuation. Script code between durable calls must be pure.
                } catch (LeaseLostException lost) {
                    emitEngine(
                            EngineEventType.LEASE_LOST,
                            workflowId,
                            null,
                            lost.getMessage(),
                            lost);
                    return;
                }
            }
            throw new IllegalStateException("workflow exceeded 10,000 immediate durable transitions: " + workflowId);
        } finally {
            if (leaseGuard != null) leaseGuard.close();
            if (workflowLease != null) {
                try {
                    coordinatedStore.releaseWorkflowLease(workflowLease);
                } catch (RuntimeException ignored) {
                    // Expiry and fencing are the recovery mechanism.
                }
            }
            driving.remove(workflowId);
        }
    }

    private WorkflowRecord blockedRecord(
            WorkflowRecord current, byte[] continuation, DurableCommand command, Instant now) {
        if (command instanceof DurableCommand.AwaitSignal awaiting) {
            int index = firstSignal(current.signals(), awaiting.name());
            if (index >= 0) {
                SignalEnvelope signal = current.signals().get(index);
                List<SignalEnvelope> remaining = new ArrayList<>(current.signals());
                remaining.remove(index);
                return current.next(
                        WorkflowStatus.READY,
                        continuation,
                        null,
                        new ResumeValue.Success(signal.payload()),
                        null,
                        null,
                        0,
                        now,
                        remaining,
                        new HistoryEntry(now, "SIGNAL_CONSUMED", awaiting.name()),
                        now);
            }
        }
        Instant availableAt;
        if (command instanceof DurableCommand.Step) {
            availableAt = now;
        } else if (command instanceof DurableCommand.Sleep sleep) {
            availableAt = safePlus(now, sleep.duration());
        } else if (command instanceof DurableCommand.AwaitSignal) {
            availableAt = null;
        } else {
            throw new IllegalStateException("unsupported durable command: " + command.getClass().getName());
        }
        return current.next(
                WorkflowStatus.WAITING,
                continuation,
                command,
                null,
                null,
                null,
                0,
                availableAt,
                current.signals(),
                new HistoryEntry(now, "SUSPENDED", describe(command)),
                now);
    }

    private void dispatchCoordinatedStep(String invocationId) {
        if (!stepPermits.tryAcquire()) return;
        if (inFlight.putIfAbsent(invocationId, Boolean.TRUE) != null) {
            stepPermits.release();
            return;
        }
        Optional<ClaimedStep> claimed;
        try {
            claimed = coordinatedStore.tryClaimStep(invocationId, instanceId, stepLeaseDuration);
        } catch (RuntimeException failure) {
            inFlight.remove(invocationId);
            stepPermits.release();
            throw failure;
        }
        if (claimed.isEmpty()) {
            inFlight.remove(invocationId);
            stepPermits.release();
            return;
        }

        ClaimedStep claim = claimed.get();
        StepInvocationRecord invocation = claim.invocation();
        StepLease stepLease = claim.lease();
        try {
            failureInjector.hit(Failpoint.AFTER_STEP_CLAIMED, invocation.workflowId(), invocationId);
            recordCoordinatedDispatch(invocation);

            AtomicBoolean leaseLost = new AtomicBoolean();
            LeaseGuard leaseGuard = startLeaseGuard(stepLeaseDuration, () -> {
                boolean renewed = coordinatedStore.renewStepLease(stepLease, stepLeaseDuration);
                if (!renewed) leaseLost.set(true);
                return renewed;
            });
            EngineStepContext context = new EngineStepContext(invocation.workflowId(), leaseLost, invocation.deadline());

            StepHandler handler = steps.find(invocation.stepName());
            failureInjector.hit(Failpoint.BEFORE_STEP_HANDLER, invocation.workflowId(), invocationId);
            StepRequest request = new StepRequest(
                    invocation.workflowId(),
                    invocation.invocationId(),
                    invocation.sequence(),
                    invocation.stepName(),
                    invocation.argument(),
                    invocation.attempt(),
                    invocation.startedAt(),
                    invocation.deadline());
            CompletionStage<? extends Serializable> stage = invokeHandlerAsync(
                    handler,
                    request,
                    context,
                    requireRecord(invocation.workflowId()).telemetryContext());
            stage.whenCompleteAsync((value, failure) -> {
                leaseGuard.close();
                try {
                    failureInjector.hit(Failpoint.AFTER_STEP_HANDLER, invocation.workflowId(), invocationId);
                    if (!leaseLost.get()) {
                        completeCoordinatedStep(
                                invocation,
                                stepLease,
                                value,
                                unwrapCompletionFailure(failure));
                    }
                } finally {
                    inFlight.remove(invocationId);
                    stepPermits.release();
                }
            }, executor);
        } catch (RuntimeException | Error failure) {
            inFlight.remove(invocationId);
            stepPermits.release();
            try {
                coordinatedStore.releaseStepLease(stepLease);
            } catch (RuntimeException releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
            throw failure;
        }
    }

    private void recordCoordinatedDispatch(StepInvocationRecord invocation) {
        for (int retry = 0; retry < CAS_RETRIES; retry++) {
            WorkflowRecord current = requireRecord(invocation.workflowId());
            if (current.status() != WorkflowStatus.WAITING
                    || !(current.pendingCommand() instanceof DurableCommand.Step step)
                    || step.sequence() != invocation.sequence()) {
                return;
            }
            Instant now = currentTime();
            WorkflowRecord replacement = current.next(
                    WorkflowStatus.WAITING,
                    current.continuation(),
                    current.pendingCommand(),
                    null,
                    null,
                    null,
                    invocation.attempt(),
                    current.availableAt(),
                    current.signals(),
                    new HistoryEntry(
                            now,
                            "STEP_DISPATCHED",
                            invocation.stepName() + " " + invocation.invocationId()
                                    + " attempt=" + invocation.attempt()),
                    now);
            try {
                WorkflowRecord stored = store.replace(
                        current.id(), current.revision(), replacement);
                emitWorkflow(current, stored);
                emitStep(
                        StepEventType.DISPATCHED,
                        stored,
                        invocation.invocationId(),
                        invocation.stepName(),
                        invocation.attempt(),
                        invocation.options().maxAttempts(),
                        invocation.startedAt(),
                        null,
                        now,
                        null);
                return;
            } catch (ConcurrentWorkflowUpdateException ignored) {
                // Retry the audit transition without affecting the claimed step lease.
            }
        }
    }

    private void completeCoordinatedStep(
            StepInvocationRecord invocation,
            StepLease stepLease,
            Serializable value,
            Throwable failure) {
        for (int retry = 0; retry < CAS_RETRIES; retry++) {
            WorkflowRecord current = requireRecord(invocation.workflowId());
            Instant now = currentTime();
            if (current.status() != WorkflowStatus.WAITING
                    || !(current.pendingCommand() instanceof DurableCommand.Step command)
                    || command.sequence() != invocation.sequence()) {
                WorkflowRecord audited = current.next(
                        current.status(),
                        current.continuation(),
                        current.pendingCommand(),
                        current.resumeValue(),
                        current.output(),
                        current.failure(),
                        current.attempt(),
                        current.availableAt(),
                        current.signals(),
                        new HistoryEntry(
                                now, "STEP_COMPLETION_IGNORED", invocation.invocationId()),
                        now);
                try {
                    coordinatedStore.completeStep(
                            stepLease,
                            current.revision(),
                            audited,
                            invocation.cancelled(now));
                } catch (ConcurrentWorkflowUpdateException ignored) {
                    continue;
                } catch (LeaseLostException ignored) {
                    // A newer owner or terminal workflow already won.
                }
                return;
            }

            WorkflowRecord workflowReplacement;
            StepInvocationRecord stepReplacement;
            if (failure == null) {
                if (value != null) codec.encode(value);
                workflowReplacement = current.next(
                        WorkflowStatus.READY,
                        current.continuation(),
                        null,
                        new ResumeValue.Success(value),
                        null,
                        null,
                        invocation.attempt(),
                        now,
                        current.signals(),
                        new HistoryEntry(
                                now,
                                "STEP_COMPLETED",
                                command.name() + " " + invocation.invocationId()),
                        now);
                stepReplacement = invocation.succeeded(value, now);
            } else if (invocation.attempt() < invocation.options().maxAttempts()) {
                Duration backoff = invocation.options().backoffForAttempt(invocation.attempt());
                Instant due = safePlus(now, backoff);
                workflowReplacement = current.next(
                        WorkflowStatus.WAITING,
                        current.continuation(),
                        command,
                        null,
                        null,
                        null,
                        invocation.attempt(),
                        due,
                        current.signals(),
                        new HistoryEntry(
                                now,
                                "STEP_RETRY",
                                command.name() + " " + invocation.invocationId()
                                        + " after=" + backoff + " cause=" + failure),
                        now);
                stepReplacement = invocation.retry(due, now);
            } else {
                FailureData failureData = FailureData.from(failure);
                workflowReplacement = current.next(
                        WorkflowStatus.DEAD_LETTERED,
                        current.continuation(),
                        command,
                        null,
                        null,
                        failureData,
                        invocation.attempt(),
                        now,
                        current.signals(),
                        new HistoryEntry(
                                now,
                                "DEAD_LETTERED",
                                command.name() + " " + invocation.invocationId()
                                        + " exhausted retries cause=" + failure),
                        now);
                stepReplacement = invocation.failed(failureData, now);
            }

            try {
                WorkflowRecord stored = coordinatedStore.completeStep(
                        stepLease,
                        current.revision(),
                        workflowReplacement,
                        stepReplacement);
                failureInjector.hit(
                        Failpoint.AFTER_STEP_RESULT_STORED,
                        invocation.workflowId(),
                        invocation.invocationId());
                emitWorkflow(current, stored);
                emitStep(
                        completionEventType(failure, stored.status()),
                        stored,
                        invocation.invocationId(),
                        invocation.stepName(),
                        invocation.attempt(),
                        invocation.options().maxAttempts(),
                        invocation.startedAt(),
                        failure,
                        now,
                        null);
                if (stored.status() == WorkflowStatus.READY) drive(stored.id());
                return;
            } catch (ConcurrentWorkflowUpdateException ignored) {
                // Re-read the workflow and retry the atomic completion.
            } catch (LeaseLostException ignored) {
                return;
            }
        }
        throw new IllegalStateException(
                "could not persist step completion: " + invocation.invocationId());
    }

    private void dispatchStep(String workflowId) {
        WorkflowRecord current = requireRecord(workflowId);
        if (current.status() != WorkflowStatus.WAITING
                || !(current.pendingCommand() instanceof DurableCommand.Step command)
                || !isDue(current, currentTime())) {
            return;
        }
        String invocationId = invocationId(workflowId, command.sequence());
        if (!stepPermits.tryAcquire()) return;
        if (inFlight.putIfAbsent(invocationId, Boolean.TRUE) != null) {
            stepPermits.release();
            return;
        }

        WorkflowRecord dispatched;
        try {
            Instant now = currentTime();
            WorkflowRecord replacement = current.next(
                    WorkflowStatus.WAITING,
                    current.continuation(),
                    command,
                    null,
                    null,
                    null,
                    current.attempt() + 1,
                    current.availableAt(),
                    current.signals(),
                    new HistoryEntry(
                            now,
                            "STEP_DISPATCHED",
                            command.name() + " " + invocationId
                                    + " attempt=" + (current.attempt() + 1)),
                    now);
            dispatched = store.replace(workflowId, current.revision(), replacement);
            emitWorkflow(current, dispatched);
            emitStep(
                    StepEventType.DISPATCHED,
                    dispatched,
                    invocationId,
                    command.name(),
                    dispatched.attempt(),
                    command.options().maxAttempts(),
                    dispatched.updatedAt(),
                    null,
                    dispatched.updatedAt(),
                    null);
        } catch (ConcurrentWorkflowUpdateException ignored) {
            inFlight.remove(invocationId);
            stepPermits.release();
            return;
        } catch (RuntimeException | Error failure) {
            inFlight.remove(invocationId);
            stepPermits.release();
            throw failure;
        }

        try {
            StepHandler handler = steps.find(command.name());
            StepRequest request = new StepRequest(
                    workflowId,
                    invocationId,
                    command.sequence(),
                    command.name(),
                    command.argument(),
                    dispatched.attempt(),
                    dispatched.updatedAt(),
                    safePlus(dispatched.updatedAt(), command.options().executionTimeout()));
            CompletionStage<? extends Serializable> stage = invokeHandlerAsync(
                    handler,
                    request,
                    new EngineStepContext(
                            workflowId, new AtomicBoolean(), request.deadline()),
                    dispatched.telemetryContext());
            stage.whenCompleteAsync(
                    (value, failure) -> completeStep(
                            workflowId,
                            invocationId,
                            value,
                            unwrapCompletionFailure(failure)),
                    executor);
        } catch (RuntimeException | Error failure) {
            inFlight.remove(invocationId);
            stepPermits.release();
            throw failure;
        }
    }

    private CompletionStage<? extends Serializable> invokeHandlerAsync(
            StepHandler handler,
            StepRequest request,
            StepContext context,
            Map<String, String> telemetryContext) {
        CompletableFuture<Serializable> result = new CompletableFuture<>();
        try {
            handlerExecutor.execute(() -> {
                if (context.isCancelled()) {
                    result.completeExceptionally(new java.util.concurrent.CancellationException(
                            "step lease or workflow was cancelled before handler execution"));
                    return;
                }
                if (handler == null) {
                    result.completeExceptionally(new IllegalStateException(
                            "no step handler registered for '" + request.stepName() + "'"));
                    return;
                }
                CompletionStage<? extends Serializable> handlerStage;
                try (WorkflowContextPropagator.Scope ignored =
                        contextPropagator.restore(telemetryContext)) {
                    handlerStage = Objects.requireNonNull(
                            handler.execute(request, context),
                            "step handler returned null CompletionStage");
                } catch (Throwable failure) {
                    result.completeExceptionally(failure);
                    return;
                }
                handlerStage.whenComplete((value, failure) -> {
                    Throwable unwrapped = unwrapCompletionFailure(failure);
                    if (unwrapped == null) result.complete(value);
                    else result.completeExceptionally(unwrapped);
                });
            });
        } catch (RuntimeException rejected) {
            result.completeExceptionally(rejected);
        }
        return result;
    }

    private void completeStep(
            String workflowId, String invocationId, Serializable value, Throwable failure) {
        try {
            for (int retry = 0; retry < CAS_RETRIES; retry++) {
                WorkflowRecord current = requireRecord(workflowId);
                if (current.status() != WorkflowStatus.WAITING
                        || !(current.pendingCommand() instanceof DurableCommand.Step command)
                        || !invocationId.equals(
                                invocationId(workflowId, command.sequence()))) {
                    return; // Late or duplicate completion.
                }
                Instant now = currentTime();
                WorkflowRecord replacement;
                if (failure == null) {
                    if (value != null) codec.encode(value);
                    replacement = current.next(
                            WorkflowStatus.READY,
                            current.continuation(),
                            null,
                            new ResumeValue.Success(value),
                            null,
                            null,
                            current.attempt(),
                            now,
                            current.signals(),
                            new HistoryEntry(
                                    now,
                                    "STEP_COMPLETED",
                                    command.name() + " " + invocationId),
                            now);
                } else if (current.attempt() < command.options().maxAttempts()) {
                    Duration backoff = command.options().backoffForAttempt(current.attempt());
                    Instant due = safePlus(now, backoff);
                    replacement = current.next(
                            WorkflowStatus.WAITING,
                            current.continuation(),
                            command,
                            null,
                            null,
                            null,
                            current.attempt(),
                            due,
                            current.signals(),
                            new HistoryEntry(
                                    now,
                                    "STEP_RETRY",
                                    command.name() + " " + invocationId
                                            + " after=" + backoff + " cause=" + failure),
                            now);
                } else {
                    FailureData failureData = FailureData.from(failure);
                    replacement = current.next(
                            WorkflowStatus.DEAD_LETTERED,
                            current.continuation(),
                            command,
                            null,
                            null,
                            failureData,
                            current.attempt(),
                            now,
                            current.signals(),
                            new HistoryEntry(
                                    now,
                                    "DEAD_LETTERED",
                                    command.name() + " " + invocationId
                                            + " exhausted retries cause=" + failure),
                            now);
                }
                try {
                    WorkflowRecord stored = store.replace(
                            workflowId, current.revision(), replacement);
                    emitWorkflow(current, stored);
                    emitStep(
                            completionEventType(failure, stored.status()),
                            stored,
                            invocationId,
                            command.name(),
                            stored.attempt(),
                            command.options().maxAttempts(),
                            current.updatedAt(),
                            failure,
                            now,
                            null);
                    if (stored.status() == WorkflowStatus.READY) drive(workflowId);
                    return;
                } catch (ConcurrentWorkflowUpdateException ignored) {
                    // Retry; completion is idempotently correlated to the invocation id.
                }
            }
            throw new IllegalStateException(
                    "could not persist step completion: " + invocationId);
        } finally {
            inFlight.remove(invocationId);
            stepPermits.release();
        }
    }

    private static StepEventType completionEventType(
            Throwable failure, WorkflowStatus workflowStatus) {
        if (failure == null) return StepEventType.COMPLETED;
        return workflowStatus == WorkflowStatus.DEAD_LETTERED
                ? StepEventType.FAILED
                : StepEventType.RETRY_SCHEDULED;
    }

    private void awakenTimer(String workflowId) {
        WorkflowLease lease = acquireWorkflowLease(workflowId);
        if (coordinatedStore != null && lease == null) return;
        try {
            for (int retry = 0; retry < CAS_RETRIES; retry++) {
                WorkflowRecord current = requireRecord(workflowId);
                if (current.status() != WorkflowStatus.WAITING
                        || !(current.pendingCommand() instanceof DurableCommand.Sleep sleep)
                        || !isDue(current, currentTime())) {
                    return;
                }
                Instant now = currentTime();
                WorkflowRecord replacement = current.next(
                        WorkflowStatus.READY,
                        current.continuation(),
                        null,
                        new ResumeValue.Success(null),
                        null,
                        null,
                        0,
                        now,
                        current.signals(),
                        new HistoryEntry(now, "TIMER_FIRED", sleep.duration().toString()),
                        now);
                try {
                    WorkflowRecord stored;
                    if (coordinatedStore != null) {
                        stored = coordinatedStore.replaceWithLease(
                                lease, current.revision(), replacement);
                    } else {
                        stored = store.replace(
                                workflowId, current.revision(), replacement);
                    }
                    emitWorkflow(current, stored);
                    drive(workflowId);
                    return;
                } catch (ConcurrentWorkflowUpdateException ignored) {
                } catch (LeaseLostException ignored) {
                    return;
                }
            }
        } finally {
            releaseWorkflowLease(lease);
        }
    }

    private void deliverQueuedSignal(String workflowId) {
        WorkflowLease lease = acquireWorkflowLease(workflowId);
        if (coordinatedStore != null && lease == null) return;
        try {
            for (int retry = 0; retry < CAS_RETRIES; retry++) {
                WorkflowRecord current = requireRecord(workflowId);
                if (current.status() != WorkflowStatus.WAITING
                        || !(current.pendingCommand() instanceof DurableCommand.AwaitSignal awaiting)) {
                    return;
                }
                int index = firstSignal(current.signals(), awaiting.name());
                if (index < 0) return;
                SignalEnvelope signal = current.signals().get(index);
                List<SignalEnvelope> remaining = new ArrayList<>(current.signals());
                remaining.remove(index);
                Instant now = currentTime();
                WorkflowRecord replacement = current.next(
                        WorkflowStatus.READY,
                        current.continuation(),
                        null,
                        new ResumeValue.Success(signal.payload()),
                        null,
                        null,
                        0,
                        now,
                        remaining,
                        new HistoryEntry(now, "SIGNAL_CONSUMED", awaiting.name()),
                        now);
                try {
                    WorkflowRecord stored;
                    if (coordinatedStore != null) {
                        stored = coordinatedStore.replaceWithLease(
                                lease, current.revision(), replacement);
                    } else {
                        stored = store.replace(
                                workflowId, current.revision(), replacement);
                    }
                    emitWorkflow(current, stored);
                    drive(workflowId);
                    return;
                } catch (ConcurrentWorkflowUpdateException ignored) {
                } catch (LeaseLostException ignored) {
                    return;
                }
            }
        } finally {
            releaseWorkflowLease(lease);
        }
    }

    private void persistEngineFailure(
            WorkflowRecord current, Throwable failure, WorkflowLease workflowLease) {
        Instant now = currentTime();
        WorkflowRecord replacement = current.next(
                WorkflowStatus.DEAD_LETTERED,
                current.continuation(),
                current.pendingCommand(),
                current.resumeValue(),
                null,
                FailureData.from(failure),
                current.attempt(),
                current.availableAt(),
                current.signals(),
                new HistoryEntry(now, "DEAD_LETTERED", "engine failure: " + failure),
                now);
        try {
            WorkflowRecord stored = coordinatedStore != null && workflowLease != null
                    ? coordinatedStore.replaceWithLease(
                            workflowLease, current.revision(), replacement)
                    : store.replace(current.id(), current.revision(), replacement);
            emitWorkflow(current, stored);
        } catch (ConcurrentWorkflowUpdateException | LeaseLostException ignored) {
            // A concurrent actor or newer lease advanced the workflow. Its durable state wins.
        }
    }

    private WorkflowLease acquireWorkflowLease(String workflowId) {
        if (coordinatedStore == null) return null;
        return coordinatedStore
                .tryAcquireWorkflowLease(workflowId, instanceId, workflowLeaseDuration)
                .orElse(null);
    }

    private void releaseWorkflowLease(WorkflowLease lease) {
        if (lease == null || coordinatedStore == null) return;
        try {
            coordinatedStore.releaseWorkflowLease(lease);
        } catch (RuntimeException ignored) {
            // Expiry and fencing are the recovery mechanism.
        }
    }

    private CompiledWorkflow verifyDefinition(WorkflowRecord record) {
        CompiledWorkflow definition = workflows.find(
                record.definitionId(), record.definitionVersion(), record.definitionHash());
        if (definition == null) {
            String available = workflows.versions(record.definitionId()).stream()
                    .map(candidate -> candidate.definition().version() + ":" + candidate.hash())
                    .collect(java.util.stream.Collectors.joining(", "));
            throw new DefinitionMismatchException(
                    "workflow '" + record.id() + "' requires " + record.definitionId() + "@"
                            + record.definitionVersion() + " hash=" + record.definitionHash()
                            + "; registered versions are [" + available + "]");
        }
        return definition;
    }

    private static Outcome runWithCpsCategories(Continuable continuation, Outcome resume) {
        return continuation.run0(resume, Continuable.categories);
    }

    private static Outcome toOutcome(ResumeValue value) {
        if (value instanceof ResumeValue.Success success) {
            return new Outcome(success.value(), null);
        }
        if (value instanceof ResumeValue.Failure failed) {
            FailureData failure = failed.failure();
            return new Outcome(null, new DurableStepException(failure.type(), failure.message()));
        }
        throw new IllegalStateException("READY workflow has no resume value");
    }

    private WorkflowRecord requireRecord(String workflowId) {
        return store.load(workflowId)
                .orElseThrow(() -> new IllegalArgumentException("workflow does not exist: " + workflowId));
    }

    private void mutate(String workflowId, java.util.function.UnaryOperator<WorkflowRecord> mutation) {
        for (int retry = 0; retry < CAS_RETRIES; retry++) {
            WorkflowRecord current = requireRecord(workflowId);
            WorkflowRecord replacement = mutation.apply(current);
            if (replacement == current) return;
            try {
                store.replace(workflowId, current.revision(), replacement);
                return;
            } catch (ConcurrentWorkflowUpdateException ignored) {
            }
        }
        throw new IllegalStateException("could not update workflow after concurrent changes: " + workflowId);
    }

    private WorkflowView view(WorkflowRecord record) {
        String waitingOn = record.pendingCommand() == null ? null : describe(record.pendingCommand());
        return new WorkflowView(
                record.id(),
                record.definitionId(),
                record.definitionVersion(),
                record.status(),
                record.revision(),
                record.output(),
                record.failure() == null ? null : record.failure().type(),
                record.failure() == null ? null : record.failure().message(),
                waitingOn,
                record.attempt(),
                record.availableAt(),
                record.signals().size(),
                record.status().recoverable(),
                record.createdAt(),
                record.updatedAt(),
                record.history().stream().map(HistoryEntry::toString).toList());
    }

    private boolean isDue(WorkflowRecord record, Instant now) {
        return record.availableAt() == null || !record.availableAt().isAfter(now);
    }

    private boolean isRunnableNow(WorkflowRecord record, Instant now) {
        if (record.status() == WorkflowStatus.READY) return true;
        if (record.status() != WorkflowStatus.WAITING || record.pendingCommand() == null) return false;
        if (record.pendingCommand() instanceof DurableCommand.Step
                || record.pendingCommand() instanceof DurableCommand.Sleep) {
            return isDue(record, now);
        }
        if (record.pendingCommand() instanceof DurableCommand.AwaitSignal awaiting) {
            return firstSignal(record.signals(), awaiting.name()) >= 0;
        }
        return false;
    }

    private static int firstSignal(List<SignalEnvelope> signals, String name) {
        for (int i = 0; i < signals.size(); i++) {
            if (signals.get(i).name().equals(name)) return i;
        }
        return -1;
    }

    private static String invocationId(String workflowId, long sequence) {
        return workflowId + ":" + sequence;
    }

    private static String describe(DurableCommand command) {
        if (command instanceof DurableCommand.Step step) {
            return "step:" + step.name() + "#" + step.sequence();
        }
        if (command instanceof DurableCommand.Sleep sleep) {
            return "sleep:" + sleep.duration() + "#" + sleep.sequence();
        }
        if (command instanceof DurableCommand.AwaitSignal signal) {
            return "signal:" + signal.name() + "#" + signal.sequence();
        }
        throw new IllegalStateException("unsupported durable command: " + command.getClass().getName());
    }

    private static Instant safePlus(Instant instant, Duration duration) {
        try {
            return instant.plus(duration);
        } catch (ArithmeticException e) {
            return Instant.MAX;
        }
    }

    private static Throwable unwrapCompletionFailure(Throwable failure) {
        if (failure == null) return null;
        if ((failure instanceof java.util.concurrent.CompletionException
                        || failure instanceof java.util.concurrent.ExecutionException)
                && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }


    private void emitWorkflow(WorkflowRecord previous, WorkflowRecord current) {
        if (listeners.isEmpty()) return;
        HistoryEntry latest = current.history().isEmpty()
                ? null
                : current.history().get(current.history().size() - 1);
        String action = latest == null ? "STATE_CHANGED" : latest.type();
        WorkflowEventType type = workflowEventType(previous, current, action);
        WorkflowEvent event = new WorkflowEvent(
                type,
                instanceId,
                current.id(),
                current.definitionId(),
                current.definitionVersion(),
                previous == null ? null : previous.status(),
                current.status(),
                current.revision(),
                current.updatedAt(),
                current.createdAt(),
                nonNegativeDuration(current.createdAt(), current.updatedAt()),
                latest == null ? "" : latest.detail(),
                current.telemetryContext());
        for (WorkflowListener listener : listeners) {
            try {
                listener.onWorkflow(event);
            } catch (Throwable failure) {
                notifyListenerFailure(listener, current.id(), null, failure);
            }
        }
    }

    private static WorkflowEventType workflowEventType(
            WorkflowRecord previous, WorkflowRecord current, String action) {
        return switch (action) {
            case "STARTED" -> WorkflowEventType.STARTED;
            case "SUSPENDED" -> WorkflowEventType.SUSPENDED;
            case "SIGNAL_RECEIVED" -> WorkflowEventType.SIGNAL_RECEIVED;
            case "SIGNAL_DELIVERED", "SIGNAL_CONSUMED" -> WorkflowEventType.SIGNAL_DELIVERED;
            case "TIMER_FIRED" -> WorkflowEventType.TIMER_COMPLETED;
            case "COMPLETED" -> WorkflowEventType.COMPLETED;
            case "FAILED" -> WorkflowEventType.FAILED;
            case "DEAD_LETTERED", "ENGINE_FAILED" -> WorkflowEventType.DEAD_LETTERED;
            case "RECOVERED", "STEP_RECOVERED", "FORCED_RESUME" -> WorkflowEventType.RECOVERED;
            case "CANCELLED" -> WorkflowEventType.CANCELLED;
            default -> {
                if (previous != null
                        && previous.status() == WorkflowStatus.READY
                        && current.status() == WorkflowStatus.WAITING) {
                    yield WorkflowEventType.SUSPENDED;
                }
                if (previous != null
                        && previous.status() == WorkflowStatus.WAITING
                        && current.status() == WorkflowStatus.READY) {
                    yield WorkflowEventType.RESUMED;
                }
                yield WorkflowEventType.STATE_CHANGED;
            }
        };
    }

    private void emitStep(
            StepEventType type,
            WorkflowRecord workflow,
            String invocationId,
            String stepName,
            int attempt,
            int maximumAttempts,
            Instant startedAt,
            Throwable failure,
            Instant at,
            String messageOverride) {
        if (listeners.isEmpty()) return;
        Throwable actual = unwrapCompletionFailure(failure);
        StepEvent event = new StepEvent(
                type,
                instanceId,
                workflow.id(),
                invocationId,
                stepName,
                attempt,
                maximumAttempts,
                at,
                startedAt,
                startedAt == null ? Duration.ZERO : nonNegativeDuration(startedAt, at),
                actual == null ? "" : actual.getClass().getName(),
                messageOverride != null
                        ? messageOverride
                        : actual == null ? "" : String.valueOf(actual.getMessage()),
                workflow.telemetryContext());
        for (WorkflowListener listener : listeners) {
            try {
                listener.onStep(event);
            } catch (Throwable listenerFailure) {
                notifyListenerFailure(listener, workflow.id(), invocationId, listenerFailure);
            }
        }
    }

    private void emitEngine(
            EngineEventType type,
            String workflowId,
            String invocationId,
            String message,
            Throwable failure) {
        if (listeners.isEmpty()) return;
        EngineEvent event = new EngineEvent(
                type, instanceId, workflowId, invocationId, eventTime(), message, failure);
        for (WorkflowListener listener : listeners) {
            try {
                listener.onEngine(event);
            } catch (Throwable ignored) {
                // Engine-event listener failures cannot safely recurse into another listener-failure event.
            }
        }
    }

    private void notifyListenerFailure(
            WorkflowListener failed,
            String workflowId,
            String invocationId,
            Throwable failure) {
        EngineEvent event = new EngineEvent(
                EngineEventType.LISTENER_FAILURE,
                instanceId,
                workflowId,
                invocationId,
                eventTime(),
                "workflow listener failed: " + failed.getClass().getName(),
                failure);
        for (WorkflowListener listener : listeners) {
            if (listener == failed) continue;
            try {
                listener.onEngine(event);
            } catch (Throwable ignored) {
                // Observability must never change workflow correctness.
            }
        }
    }

    private Instant eventTime() {
        try {
            return currentTime();
        } catch (RuntimeException ignored) {
            return clock.instant();
        }
    }

    private static Duration nonNegativeDuration(Instant start, Instant end) {
        return start == null || end == null || end.isBefore(start)
                ? Duration.ZERO
                : Duration.between(start, end);
    }

    private void cleanupFailedConstruction(Throwable original) {
        cancelPoller();
        cancelMaintenance();
        if (ownsPollExecutor) shutdownExecutor(pollExecutor);
        if (ownsLeaseExecutor) shutdownExecutor(leaseExecutor);
        if (ownsMaintenanceExecutor) shutdownExecutor(maintenanceExecutor);
        if (ownsHandlerExecutor) shutdownExecutor(handlerExecutor);
        if (ownsExecutor) shutdownExecutor(executor);
        try {
            workflows.close();
        } catch (RuntimeException closeFailure) {
            original.addSuppressed(closeFailure);
        }
    }

    private void runSchemaPreflight() {
        if (schemaPreflightMode == SchemaPreflightMode.OFF
                || !(store instanceof SchemaManagedWorkflowStore managed)) {
            return;
        }
        SchemaStatus status = schemaPreflightMode == SchemaPreflightMode.MIGRATE
                ? managed.migrateSchema()
                : managed.schemaStatus();
        if (status.compatible()) return;
        if (schemaPreflightMode == SchemaPreflightMode.FAIL
                || schemaPreflightMode == SchemaPreflightMode.MIGRATE) {
            throw new SchemaPreflightException(status);
        }
        emitEngine(
                EngineEventType.SCHEMA_PREFLIGHT_WARNING,
                null,
                null,
                status.message(),
                null);
    }

    private void validateRetentionConfiguration() {
        if (retentionPolicy != null && !(store instanceof ArchiveCapableWorkflowStore)) {
            throw new IllegalStateException("retention policy requires an archive-capable store");
        }
    }

    private void registerStepConcurrencyLimits() {
        if (stepConcurrencyLimits.isEmpty()) return;
        if (!(store instanceof StepConcurrencyStore concurrencyStore)) {
            throw new IllegalStateException(
                    "configured step concurrency limits require a StepConcurrencyStore");
        }
        stepConcurrencyLimits.forEach(concurrencyStore::registerStepConcurrencyLimit);
    }

    private void runRetentionSafely() {
        if (closed.get() || lifecycle.get() != EngineLifecycleState.RUNNING || retentionPolicy == null) return;
        activeMaintenance.incrementAndGet();
        try {
            if (closed.get() || lifecycle.get() != EngineLifecycleState.RUNNING) return;
            ArchiveCapableWorkflowStore archiveStore = (ArchiveCapableWorkflowStore) store;
            Instant cutoff = currentTime().minus(retentionPolicy.terminalAge());
            ArchiveResult result = archiveStore.archive(new ArchiveRequest(
                    retentionPolicy.statuses(), cutoff, retentionPolicy.batchSize()));
            if (result.count() > 0) {
                emitEngine(EngineEventType.RETENTION_ARCHIVED, null, null,
                        "archived " + result.count() + " terminal workflow(s)", null);
            }
        } catch (Throwable failure) {
            emitEngine(EngineEventType.MAINTENANCE_FAILURE, null, null,
                    "retention maintenance failed", failure);
        } finally {
            activeMaintenance.decrementAndGet();
        }
    }

    private void runDefinitionPreflight() {
        if (definitionPreflightMode == DefinitionPreflightMode.OFF) return;
        DefinitionVerificationReport report = verifyDefinitionsInternal();
        if (report.compatible()) return;
        if (definitionPreflightMode == DefinitionPreflightMode.FAIL) {
            throw new DefinitionPreflightException(report);
        }
        emitEngine(
                EngineEventType.DEFINITION_PREFLIGHT_WARNING,
                null,
                null,
                report.incompatibleWorkflows() + " persisted workflow(s) cannot be resumed by registered definitions",
                null);
    }

    private DefinitionVerificationReport verifyDefinitionsInternal() {
        Instant checkedAt = eventTime();
        List<StoredDefinitionReference> inventory;
        if (store instanceof DefinitionInventoryStore optimized) {
            inventory = optimized.definitionInventory();
        } else {
            Map<DefinitionInventoryKey, Long> grouped = new LinkedHashMap<>();
            for (WorkflowRecord record : store.list()) {
                if (!requiresDefinition(record.status())) continue;
                DefinitionInventoryKey key = new DefinitionInventoryKey(
                        record.definitionId(),
                        record.definitionVersion(),
                        record.definitionHash(),
                        record.status());
                grouped.merge(key, 1L, Long::sum);
            }
            inventory = grouped.entrySet().stream()
                    .map(entry -> new StoredDefinitionReference(
                            entry.getKey().definitionId(),
                            entry.getKey().definitionVersion(),
                            entry.getKey().definitionHash(),
                            entry.getKey().status(),
                            entry.getValue()))
                    .toList();
        }

        long total = 0L;
        long compatible = 0L;
        List<DefinitionCompatibilityIssue> issues = new ArrayList<>();
        for (StoredDefinitionReference reference : inventory) {
            if (!requiresDefinition(reference.status())) continue;
            total += reference.workflowCount();
            if (workflows.contains(
                    reference.definitionId(), reference.definitionVersion(), reference.definitionHash())) {
                compatible += reference.workflowCount();
            } else {
                issues.add(new DefinitionCompatibilityIssue(
                        reference.definitionId(),
                        reference.definitionVersion(),
                        reference.definitionHash(),
                        reference.status(),
                        reference.workflowCount(),
                        "exact id/version/source hash is not registered"));
            }
        }
        return new DefinitionVerificationReport(checkedAt, total, compatible, issues);
    }

    private static boolean requiresDefinition(WorkflowStatus status) {
        return status == WorkflowStatus.READY
                || status == WorkflowStatus.WAITING
                || status == WorkflowStatus.DEAD_LETTERED;
    }

    private record DefinitionInventoryKey(
            String definitionId,
            int definitionVersion,
            String definitionHash,
            WorkflowStatus status) {}

    private WorkflowPage searchWorkflows(WorkflowQuery query) {
        Objects.requireNonNull(query, "query");
        if (store instanceof SearchableWorkflowStore searchable) {
            WorkflowRecordPage page = searchable.search(query);
            return new WorkflowPage(
                    page.items().stream().map(this::view).toList(),
                    page.total(),
                    query.limit(),
                    query.offset());
        }

        List<WorkflowRecord> matches = store.list().stream()
                .filter(record -> query.statuses().isEmpty()
                        || query.statuses().contains(record.status()))
                .filter(record -> query.definitionId() == null
                        || query.definitionId().equals(record.definitionId()))
                .filter(record -> query.createdFrom() == null
                        || !record.createdAt().isBefore(query.createdFrom()))
                .filter(record -> query.createdTo() == null
                        || !record.createdAt().isAfter(query.createdTo()))
                .filter(record -> query.updatedFrom() == null
                        || !record.updatedAt().isBefore(query.updatedFrom()))
                .filter(record -> query.updatedTo() == null
                        || !record.updatedAt().isAfter(query.updatedTo()))
                .sorted(Comparator.comparing(WorkflowRecord::updatedAt)
                        .reversed()
                        .thenComparing(WorkflowRecord::id))
                .toList();
        int from = Math.min(query.offset(), matches.size());
        int to = Math.min(matches.size(), from + query.limit());
        return new WorkflowPage(
                matches.subList(from, to).stream().map(this::view).toList(),
                matches.size(),
                query.limit(),
                query.offset());
    }

    private Map<WorkflowStatus, Long> workflowCounts() {
        if (store instanceof SearchableWorkflowStore searchable) {
            return searchable.countByStatus();
        }
        EnumMap<WorkflowStatus, Long> result = new EnumMap<>(WorkflowStatus.class);
        for (WorkflowStatus status : WorkflowStatus.values()) result.put(status, 0L);
        for (WorkflowRecord record : store.list()) result.merge(record.status(), 1L, Long::sum);
        return Map.copyOf(result);
    }

    private EngineHealth engineHealth() {
        Instant now = eventTime();
        EngineLifecycleState currentLifecycle = lifecycle.get();
        SchemaStatus schema = null;
        if (closed.get()) {
            return new EngineHealth(
                    HealthStatus.DOWN,
                    EngineLifecycleState.CLOSED,
                    false,
                    false,
                    now,
                    Map.of(),
                    activeStarts.get(),
                    activePolls.get(),
                    driving.size(),
                    inFlight.size(),
                    activeMaintenance.get(),
                    null,
                    "engine is closed");
        }
        try {
            if (store instanceof SearchableWorkflowStore searchable) searchable.checkHealth();
            else store.list();
            if (store instanceof SchemaManagedWorkflowStore managed) schema = managed.schemaStatus();
            Map<WorkflowStatus, Long> counts = workflowCounts();
            long deadLettered = counts.getOrDefault(WorkflowStatus.DEAD_LETTERED, 0L);
            boolean schemaCurrent = schema == null || schema.compatible();
            boolean degraded = deadLettered > 0
                    || currentLifecycle == EngineLifecycleState.QUIESCING
                    || !schemaCurrent;
            String message;
            if (!schemaCurrent) message = schema.message();
            else if (currentLifecycle == EngineLifecycleState.QUIESCING) message = "engine is quiescing";
            else if (deadLettered > 0) message = deadLettered + " dead-lettered workflow(s)";
            else message = "healthy";
            return new EngineHealth(
                    degraded ? HealthStatus.DEGRADED : HealthStatus.UP,
                    currentLifecycle,
                    true,
                    true,
                    now,
                    counts,
                    activeStarts.get(),
                    activePolls.get(),
                    driving.size(),
                    inFlight.size(),
                    activeMaintenance.get(),
                    schema,
                    message);
        } catch (RuntimeException failure) {
            return new EngineHealth(
                    HealthStatus.DOWN,
                    currentLifecycle,
                    true,
                    false,
                    now,
                    Map.of(),
                    activeStarts.get(),
                    activePolls.get(),
                    driving.size(),
                    inFlight.size(),
                    activeMaintenance.get(),
                    schema,
                    failure.toString());
        }
    }

    private WorkflowRecord adminReplace(
            WorkflowRecord current,
            WorkflowRecord replacement,
            StepInvocationRecord stepReplacement) {
        WorkflowRecord stored = coordinatedStore != null && stepReplacement != null
                ? coordinatedStore.recoverStep(
                        current.revision(), replacement, stepReplacement)
                : store.replace(current.id(), current.revision(), replacement);
        emitWorkflow(current, stored);
        return stored;
    }

    private final class Administration implements WorkflowAdministration {
        @Override
        public Optional<WorkflowView> get(String workflowId) {
            return DurableWorkflowEngine.this.get(workflowId);
        }

        @Override
        public WorkflowPage search(WorkflowQuery query) {
            ensureOpen();
            return searchWorkflows(query);
        }

        @Override
        public WorkflowDiagnostic diagnose(String workflowId) {
            ensureOpen();
            WorkflowRecord record = requireRecord(workflowId);
            Optional<SnapshotMetadata> metadata = Optional.empty();
            String snapshotProblem = "";
            if (record.continuation() != null && codec instanceof SnapshotCodec snapshotCodec) {
                try {
                    metadata = Optional.of(snapshotCodec.inspect(record.continuation()));
                } catch (RuntimeException failure) {
                    snapshotProblem = failure.toString();
                }
            }
            Optional<StepInvocationRecord> pending = Optional.empty();
            if (coordinatedStore != null
                    && record.pendingCommand() instanceof DurableCommand.Step step) {
                pending = coordinatedStore.loadStep(invocationId(record.id(), step.sequence()));
            }
            return new WorkflowDiagnostic(
                    view(record),
                    workflows.contains(
                            record.definitionId(), record.definitionVersion(), record.definitionHash()),
                    metadata,
                    pending,
                    record.history(),
                    snapshotProblem);
        }

        @Override
        public EngineLifecycleState lifecycle() {
            return DurableWorkflowEngine.this.lifecycle();
        }

        @Override
        public void quiesce() {
            DurableWorkflowEngine.this.quiesce();
        }

        @Override
        public void resume() {
            DurableWorkflowEngine.this.resume();
        }

        @Override
        public Optional<SchemaStatus> schemaStatus() {
            ensureOpen();
            return store instanceof SchemaManagedWorkflowStore managed
                    ? Optional.of(managed.schemaStatus())
                    : Optional.empty();
        }

        @Override
        public void cancel(String workflowId, String reason) {
            DurableWorkflowEngine.this.cancel(workflowId, reason);
        }

        @Override
        public void retry(String workflowId, String reason) {
            ensureOpen();
            for (int retry = 0; retry < CAS_RETRIES; retry++) {
                WorkflowRecord current = requireRecord(workflowId);
                if (current.status() != WorkflowStatus.DEAD_LETTERED) {
                    throw new IllegalStateException(
                            "workflow is not dead-lettered: " + workflowId);
                }
                Instant now = currentTime();
                String detail = reason == null || reason.isBlank()
                        ? "administrative retry"
                        : reason;
                StepInvocationRecord stepReplacement = null;
                WorkflowRecord replacement;
                if (current.pendingCommand() instanceof DurableCommand.Step step) {
                    if (coordinatedStore != null) {
                        String id = invocationId(workflowId, step.sequence());
                        stepReplacement = coordinatedStore
                                .loadStep(id)
                                .orElseThrow(() -> new IllegalStateException(
                                        "missing step invocation: " + id))
                                .requeued(now);
                    }
                    replacement = current.next(
                            WorkflowStatus.WAITING,
                            current.continuation(),
                            current.pendingCommand(),
                            null,
                            null,
                            null,
                            0,
                            now,
                            current.signals(),
                            new HistoryEntry(now, "STEP_RECOVERED", detail),
                            now);
                } else if (current.pendingCommand() != null) {
                    replacement = current.next(
                            WorkflowStatus.WAITING,
                            current.continuation(),
                            current.pendingCommand(),
                            null,
                            null,
                            null,
                            current.attempt(),
                            current.availableAt(),
                            current.signals(),
                            new HistoryEntry(now, "RECOVERED", detail),
                            now);
                } else {
                    ResumeValue resume = current.resumeValue() == null
                            ? new ResumeValue.Success(null)
                            : current.resumeValue();
                    replacement = current.next(
                            WorkflowStatus.READY,
                            current.continuation(),
                            null,
                            resume,
                            null,
                            null,
                            current.attempt(),
                            now,
                            current.signals(),
                            new HistoryEntry(now, "RECOVERED", detail),
                            now);
                }
                try {
                    WorkflowRecord stored = adminReplace(current, replacement, stepReplacement);
                    if (stepReplacement != null) {
                        emitStep(
                                StepEventType.RECOVERED,
                                stored,
                                stepReplacement.invocationId(),
                                stepReplacement.stepName(),
                                0,
                                stepReplacement.options().maxAttempts(),
                                null,
                                null,
                                now,
                                detail);
                    }
                    if (stored.status() == WorkflowStatus.READY) drive(workflowId);
                    return;
                } catch (ConcurrentWorkflowUpdateException ignored) {
                    // Retry against the newest durable state.
                }
            }
            throw new IllegalStateException(
                    "could not retry workflow after concurrent changes: " + workflowId);
        }

        @Override
        public void resumeWithValue(String workflowId, Serializable value, String reason) {
            forceResume(
                    workflowId,
                    new ResumeValue.Success(value),
                    value,
                    null,
                    reason);
        }

        @Override
        public void resumeWithFailure(String workflowId, Throwable failure, String reason) {
            Objects.requireNonNull(failure, "failure");
            forceResume(
                    workflowId,
                    new ResumeValue.Failure(FailureData.from(failure)),
                    null,
                    failure,
                    reason);
        }

        private void forceResume(
                String workflowId,
                ResumeValue resume,
                Serializable value,
                Throwable failure,
                String reason) {
            ensureOpen();
            if (value != null) codec.encode(value);
            for (int retry = 0; retry < CAS_RETRIES; retry++) {
                WorkflowRecord current = requireRecord(workflowId);
                if (current.status() != WorkflowStatus.DEAD_LETTERED) {
                    throw new IllegalStateException(
                            "workflow is not dead-lettered: " + workflowId);
                }
                Instant now = currentTime();
                String detail = reason == null || reason.isBlank()
                        ? "forced administrative resume"
                        : reason;
                StepInvocationRecord stepReplacement = null;
                if (current.pendingCommand() instanceof DurableCommand.Step step
                        && coordinatedStore != null) {
                    String id = invocationId(workflowId, step.sequence());
                    StepInvocationRecord invocation = coordinatedStore
                            .loadStep(id)
                            .orElseThrow(() -> new IllegalStateException(
                                    "missing step invocation: " + id));
                    stepReplacement = failure == null
                            ? invocation.succeeded(value, now)
                            : invocation.failed(FailureData.from(failure), now);
                }
                WorkflowRecord replacement = current.next(
                        WorkflowStatus.READY,
                        current.continuation(),
                        null,
                        resume,
                        null,
                        null,
                        current.attempt(),
                        now,
                        current.signals(),
                        new HistoryEntry(now, "FORCED_RESUME", detail),
                        now);
                try {
                    WorkflowRecord stored = adminReplace(current, replacement, stepReplacement);
                    if (stepReplacement != null) {
                        emitStep(
                                StepEventType.RECOVERED,
                                stored,
                                stepReplacement.invocationId(),
                                stepReplacement.stepName(),
                                stepReplacement.attempt(),
                                stepReplacement.options().maxAttempts(),
                                stepReplacement.startedAt(),
                                failure,
                                now,
                                detail);
                    }
                    drive(workflowId);
                    return;
                } catch (ConcurrentWorkflowUpdateException ignored) {
                    // Retry against the newest durable state.
                }
            }
            throw new IllegalStateException(
                    "could not resume workflow after concurrent changes: " + workflowId);
        }

        @Override
        public void deadLetter(String workflowId, String reason) {
            ensureOpen();
            for (int retry = 0; retry < CAS_RETRIES; retry++) {
                WorkflowRecord current = requireRecord(workflowId);
                if (current.status() == WorkflowStatus.DEAD_LETTERED) return;
                if (current.status().terminal()) {
                    throw new IllegalStateException(
                            "workflow is terminal: " + workflowId + " is " + current.status());
                }
                Instant now = currentTime();
                String detail = reason == null || reason.isBlank()
                        ? "administratively dead-lettered"
                        : reason;
                WorkflowRecord replacement = current.next(
                        WorkflowStatus.DEAD_LETTERED,
                        current.continuation(),
                        current.pendingCommand(),
                        current.resumeValue(),
                        null,
                        new FailureData("AdministrativeDeadLetter", detail, ""),
                        current.attempt(),
                        current.availableAt(),
                        current.signals(),
                        new HistoryEntry(now, "DEAD_LETTERED", detail),
                        now);
                String pendingInvocationId = current.pendingCommand() instanceof DurableCommand.Step step
                        ? invocationId(workflowId, step.sequence())
                        : null;
                try {
                    WorkflowRecord stored = coordinatedStore != null && pendingInvocationId != null
                            ? coordinatedStore.cancelWorkflow(
                                    current.revision(), replacement, pendingInvocationId)
                            : store.replace(workflowId, current.revision(), replacement);
                    emitWorkflow(current, stored);
                    if (pendingInvocationId != null) {
                        DurableCommand.Step step = (DurableCommand.Step) current.pendingCommand();
                        emitStep(
                                StepEventType.CANCELLED,
                                stored,
                                pendingInvocationId,
                                step.name(),
                                current.attempt(),
                                step.options().maxAttempts(),
                                null,
                                null,
                                now,
                                detail);
                    }
                    return;
                } catch (ConcurrentWorkflowUpdateException ignored) {
                    // Retry against the newest durable state.
                }
            }
            throw new IllegalStateException(
                    "could not dead-letter workflow after concurrent changes: " + workflowId);
        }

        @Override
        public DefinitionVerificationReport verifyDefinitions() {
            ensureOpen();
            return verifyDefinitionsInternal();
        }

        @Override
        public ArchiveResult archive(ArchiveRequest request) {
            ensureOpen();
            if (!(store instanceof ArchiveCapableWorkflowStore archiveStore)) {
                throw new UnsupportedOperationException("workflow store does not support archival");
            }
            return archiveStore.archive(request);
        }

        @Override
        public Optional<ArchivedWorkflow> archived(String archiveId) {
            ensureOpen();
            if (!(store instanceof ArchiveCapableWorkflowStore archiveStore)) {
                throw new UnsupportedOperationException("workflow store does not support archival");
            }
            return archiveStore.loadArchived(archiveId);
        }

        @Override
        public List<ArchivedWorkflowSummary> archived(int limit, int offset) {
            ensureOpen();
            if (!(store instanceof ArchiveCapableWorkflowStore archiveStore)) {
                throw new UnsupportedOperationException("workflow store does not support archival");
            }
            return archiveStore.listArchived(limit, offset);
        }

        @Override
        public StuckWorkReport stuckWork(Duration overdueBy, int limit) {
            ensureOpen();
            if (!(store instanceof StuckWorkStore stuckStore)) {
                throw new UnsupportedOperationException("workflow store does not support stuck-work inspection");
            }
            return stuckStore.scanStuckWork(overdueBy, limit);
        }

        @Override
        public Map<String, Integer> stepConcurrencyLimits() {
            ensureOpen();
            return store instanceof StepConcurrencyStore concurrencyStore
                    ? concurrencyStore.stepConcurrencyLimits()
                    : Map.of();
        }

        @Override
        public EngineHealth health() {
            return engineHealth();
        }
    }

    private Instant currentTime() {
        return coordinatedStore == null ? clock.instant() : coordinatedStore.currentTime();
    }

    private void tickSafely() {
        if (closed.get() || lifecycle.get() != EngineLifecycleState.RUNNING) return;
        try {
            tick();
        } catch (Throwable failure) {
            emitEngine(EngineEventType.POLL_FAILURE,null,null,"engine tick failed",failure);
        }
    }

    private void ensureOpen() {
        if (closed.get() || lifecycle.get() == EngineLifecycleState.CLOSED) {
            throw new IllegalStateException("engine is closed");
        }
    }

    private void beginStart() {
        ensureOpen();
        if (lifecycle.get() != EngineLifecycleState.RUNNING) {
            throw new IllegalStateException("engine is quiescing and does not accept new workflows");
        }
        activeStarts.incrementAndGet();
        if (closed.get() || lifecycle.get() != EngineLifecycleState.RUNNING) {
            activeStarts.decrementAndGet();
            throw new IllegalStateException("engine is quiescing and does not accept new workflows");
        }
    }

    private static void validateWorkflowId(String id) {
        if (id == null || !id.matches("[A-Za-z0-9_.-]+")) {
            throw new IllegalArgumentException("workflow id must match [A-Za-z0-9_.-]+");
        }
    }

    @Override
    public void close() {
        closeGracefully(shutdownTimeout);
    }

    private void forceClose() {
        if (!closed.compareAndSet(false, true)) return;
        lifecycle.set(EngineLifecycleState.CLOSED);
        cancelPoller();
        cancelMaintenance();
        RuntimeException failure = null;
        if (ownsPollExecutor) shutdownExecutor(pollExecutor);
        if (ownsLeaseExecutor) shutdownExecutor(leaseExecutor);
        if (ownsMaintenanceExecutor) shutdownExecutor(maintenanceExecutor);
        if (ownsHandlerExecutor) shutdownExecutor(handlerExecutor);
        if (ownsExecutor) shutdownExecutor(executor);
        try {
            workflows.close();
        } catch (RuntimeException e) {
            failure = e;
        }
        try {
            store.close();
        } catch (RuntimeException e) {
            if (failure == null) failure = e;
            else failure.addSuppressed(e);
        }
        emitEngine(EngineEventType.ENGINE_CLOSED, null, null, "engine closed", failure);
        if (failure != null) throw failure;
    }

    private void schedulePoller() {
        synchronized (pollLifecycleLock) {
            if (closed.get() || lifecycle.get() != EngineLifecycleState.RUNNING) return;
            if (pollTask != null && !pollTask.isDone()) return;
            long millis = Math.max(1L, pollInterval.toMillis());
            pollTask = pollExecutor.scheduleWithFixedDelay(
                    this::tickSafely, 0L, millis, TimeUnit.MILLISECONDS);
        }
    }

    private void cancelPoller() {
        synchronized (pollLifecycleLock) {
            ScheduledFuture<?> current = pollTask;
            pollTask = null;
            if (current != null) current.cancel(false);
        }
    }

    private void scheduleMaintenance() {
        synchronized (maintenanceLifecycleLock) {
            if (closed.get() || lifecycle.get() != EngineLifecycleState.RUNNING || retentionPolicy == null) return;
            if (maintenanceTask != null && !maintenanceTask.isDone()) return;
            long millis = Math.max(1L, retentionPolicy.interval().toMillis());
            maintenanceTask = maintenanceExecutor.scheduleWithFixedDelay(
                    this::runRetentionSafely, millis, millis, TimeUnit.MILLISECONDS);
        }
    }

    private void cancelMaintenance() {
        synchronized (maintenanceLifecycleLock) {
            ScheduledFuture<?> current = maintenanceTask;
            maintenanceTask = null;
            if (current != null) current.cancel(false);
        }
    }

    private LeaseGuard startLeaseGuard(Duration duration, java.util.function.BooleanSupplier renew) {
        long periodMillis = Math.max(50L, duration.toMillis() / 3L);
        AtomicBoolean lost = new AtomicBoolean();
        ScheduledFuture<?> task = leaseExecutor.scheduleWithFixedDelay(() -> {
            try {
                if (!renew.getAsBoolean()) lost.set(true);
            } catch (RuntimeException failure) {
                lost.set(true);
            }
        }, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
        return new LeaseGuard(task, lost);
    }

    private static ScheduledExecutorService newDaemonScheduler(String name, int threads) {
        AtomicInteger sequence = new AtomicInteger();
        return Executors.newScheduledThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable, name + "-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    private static ExecutorService newDaemonExecutor(String name, int threads) {
        AtomicInteger sequence = new AtomicInteger();
        return Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable, name + "-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    private static int maximumHandlerThreads(int maximumInFlightSteps) {
        int processors = Math.max(1, Runtime.getRuntime().availableProcessors());
        int suggested = Math.max(4, Math.min(32, Math.max(processors, maximumInFlightSteps / 8)));
        return Math.min(maximumInFlightSteps, suggested);
    }

    private static void shutdownExecutor(ExecutorService service) {
        // Engine work has already drained, or the caller explicitly accepted lease-based takeover.
        // Do not add an executor-specific wait beyond closeGracefully's single deadline.
        service.shutdownNow();
    }

    private static final class LeaseGuard implements AutoCloseable {
        private final ScheduledFuture<?> task;
        private final AtomicBoolean lost;

        private LeaseGuard(ScheduledFuture<?> task, AtomicBoolean lost) {
            this.task = task;
            this.lost = lost;
        }

        boolean lost() {
            return lost.get();
        }

        @Override
        public void close() {
            task.cancel(false);
        }
    }

    private final class EngineStepContext implements StepContext {
        private final String workflowId;
        private final AtomicBoolean leaseLost;
        private final Instant deadline;

        private EngineStepContext(
                String workflowId, AtomicBoolean leaseLost, Instant deadline) {
            this.workflowId = workflowId;
            this.leaseLost = leaseLost;
            this.deadline = deadline;
        }

        @Override
        public Clock clock() {
            return clock;
        }

        @Override
        public ScheduledExecutorService executor() {
            return executor;
        }

        @Override
        public boolean isCancelled() {
            if (leaseLost.get()) return true;
            if (deadline != null && !eventTime().isBefore(deadline)) return true;
            return store.load(workflowId)
                    .map(record -> record.status() != WorkflowStatus.WAITING)
                    .orElse(true);
        }
    }

    public static final class Builder {
        private final WorkflowStore store;
        private final List<WorkflowDefinition> definitions = new ArrayList<>();
        private final LinkedHashMap<String, StepHandler> handlers = new LinkedHashMap<>();
        private ObjectCodec codec = new JavaObjectCodec();
        private Clock clock = Clock.systemUTC();
        private ScheduledExecutorService executor;
        private ExecutorService handlerExecutor;
        private ScheduledExecutorService pollExecutor;
        private ScheduledExecutorService leaseExecutor;
        private ScheduledExecutorService maintenanceExecutor;
        private Duration pollInterval = Duration.ofMillis(100);
        private boolean automaticPolling = true;
        private ClassLoader parentClassLoader = DurableWorkflowEngine.class.getClassLoader();
        private String instanceId = defaultInstanceId();
        private Duration workflowLeaseDuration = Duration.ofSeconds(30);
        private Duration stepLeaseDuration = Duration.ofSeconds(30);
        private int claimBatchSize = 100;
        private int maximumInFlightSteps = 256;
        private Duration shutdownTimeout = Duration.ofSeconds(5);
        private DefinitionPreflightMode definitionPreflightMode = DefinitionPreflightMode.WARN;
        private SchemaPreflightMode schemaPreflightMode = SchemaPreflightMode.OFF;
        private RetentionPolicy retentionPolicy;
        private final LinkedHashMap<String, Integer> stepConcurrencyLimits = new LinkedHashMap<>();
        private FailureInjector failureInjector = FailureInjector.NONE;
        private WorkflowContextPropagator contextPropagator = WorkflowContextPropagator.NONE;
        private final List<WorkflowListener> listeners = new ArrayList<>();

        private Builder(WorkflowStore store) {
            this.store = Objects.requireNonNull(store, "store");
        }

        public Builder definition(WorkflowDefinition definition) {
            Objects.requireNonNull(definition, "definition");
            boolean duplicate = definitions.stream().anyMatch(existing ->
                    existing.id().equals(definition.id()) && existing.version() == definition.version());
            if (duplicate) {
                throw new IllegalStateException(
                        "duplicate definition: " + definition.id() + "@" + definition.version());
            }
            definitions.add(definition);
            return this;
        }

        public Builder step(String name, StepHandler handler) {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(handler, "handler");
            StepHandler previous = handlers.putIfAbsent(name, handler);
            if (previous != null) throw new IllegalStateException("duplicate step: " + name);
            return this;
        }

        public Builder codec(ObjectCodec codec) {
            this.codec = Objects.requireNonNull(codec, "codec");
            return this;
        }

        public Builder clock(Clock clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        public Builder executor(ScheduledExecutorService executor) {
            this.executor = Objects.requireNonNull(executor, "executor");
            return this;
        }

        /** Dedicated executor for entering handlers; blocking setup never consumes callback threads. */
        public Builder handlerExecutor(ExecutorService handlerExecutor) {
            this.handlerExecutor = Objects.requireNonNull(handlerExecutor, "handlerExecutor");
            return this;
        }

        /** Dedicated scheduler for polling; do not share it with blocking step-handler work. */
        public Builder pollExecutor(ScheduledExecutorService pollExecutor) {
            this.pollExecutor = Objects.requireNonNull(pollExecutor, "pollExecutor");
            return this;
        }

        /** Dedicated scheduler for lease renewal; do not share it with blocking step-handler work. */
        public Builder leaseExecutor(ScheduledExecutorService leaseExecutor) {
            this.leaseExecutor = Objects.requireNonNull(leaseExecutor, "leaseExecutor");
            return this;
        }

        /** Dedicated scheduler for retention and other bounded maintenance. */
        public Builder maintenanceExecutor(ScheduledExecutorService maintenanceExecutor) {
            this.maintenanceExecutor = Objects.requireNonNull(maintenanceExecutor, "maintenanceExecutor");
            return this;
        }

        public Builder pollInterval(Duration pollInterval) {
            Objects.requireNonNull(pollInterval, "pollInterval");
            if (pollInterval.isZero() || pollInterval.isNegative()) {
                throw new IllegalArgumentException("pollInterval must be positive");
            }
            this.pollInterval = pollInterval;
            return this;
        }

        public Builder automaticPolling(boolean automaticPolling) {
            this.automaticPolling = automaticPolling;
            return this;
        }

        public Builder parentClassLoader(ClassLoader parentClassLoader) {
            this.parentClassLoader = Objects.requireNonNull(parentClassLoader, "parentClassLoader");
            return this;
        }


        public Builder instanceId(String instanceId) {
            Objects.requireNonNull(instanceId, "instanceId");
            if (instanceId.isBlank()) throw new IllegalArgumentException("instanceId must not be blank");
            this.instanceId = instanceId;
            return this;
        }

        public Builder workflowLease(Duration duration) {
            this.workflowLeaseDuration = positive(duration, "workflowLease");
            return this;
        }

        public Builder stepLease(Duration duration) {
            this.stepLeaseDuration = positive(duration, "stepLease");
            return this;
        }

        public Builder claimBatchSize(int claimBatchSize) {
            if (claimBatchSize < 1) throw new IllegalArgumentException("claimBatchSize must be >= 1");
            this.claimBatchSize = claimBatchSize;
            return this;
        }

        public Builder maximumInFlightSteps(int maximumInFlightSteps) {
            if (maximumInFlightSteps < 1) {
                throw new IllegalArgumentException("maximumInFlightSteps must be >= 1");
            }
            this.maximumInFlightSteps = maximumInFlightSteps;
            return this;
        }

        public Builder shutdownTimeout(Duration shutdownTimeout) {
            Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
            if (shutdownTimeout.isNegative()) {
                throw new IllegalArgumentException("shutdownTimeout must not be negative");
            }
            this.shutdownTimeout = shutdownTimeout;
            return this;
        }

        public Builder definitionPreflight(DefinitionPreflightMode mode) {
            this.definitionPreflightMode = Objects.requireNonNull(mode, "mode");
            return this;
        }

        public Builder schemaPreflight(SchemaPreflightMode mode) {
            this.schemaPreflightMode = Objects.requireNonNull(mode, "mode");
            return this;
        }

        public Builder retentionPolicy(RetentionPolicy retentionPolicy) {
            this.retentionPolicy = Objects.requireNonNull(retentionPolicy, "retentionPolicy");
            return this;
        }

        /** Database-global concurrency limit for a named step across every engine replica. */
        public Builder stepConcurrencyLimit(String stepName, int maximumConcurrent) {
            Objects.requireNonNull(stepName, "stepName");
            if (stepName.isBlank()) throw new IllegalArgumentException("stepName must not be blank");
            if (maximumConcurrent < 1) {
                throw new IllegalArgumentException("maximumConcurrent must be >= 1");
            }
            Integer previous = stepConcurrencyLimits.putIfAbsent(stepName, maximumConcurrent);
            if (previous != null && previous != maximumConcurrent) {
                throw new IllegalStateException("conflicting concurrency limit for step " + stepName);
            }
            return this;
        }

        public Builder failureInjector(FailureInjector failureInjector) {
            this.failureInjector = Objects.requireNonNull(failureInjector, "failureInjector");
            return this;
        }

        public Builder contextPropagator(WorkflowContextPropagator contextPropagator) {
            this.contextPropagator = Objects.requireNonNull(contextPropagator, "contextPropagator");
            return this;
        }

        public Builder listener(WorkflowListener listener) {
            listeners.add(Objects.requireNonNull(listener, "listener"));
            return this;
        }

        private static Duration positive(Duration duration, String name) {
            Objects.requireNonNull(duration, name);
            if (duration.isZero() || duration.isNegative()) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return duration;
        }

        private static String defaultInstanceId() {
            return "engine-" + UUID.randomUUID();
        }

        public DurableWorkflowEngine build() {
            return new DurableWorkflowEngine(this);
        }
    }
}
