package io.github.durablecps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.durablecps.admin.EngineLifecycleState;
import io.github.durablecps.admin.SchemaPreflightException;
import io.github.durablecps.admin.SchemaPreflightMode;
import io.github.durablecps.api.WorkflowDefinition;
import io.github.durablecps.runtime.DurableWorkflowEngine;
import io.github.durablecps.runtime.state.WorkflowRecord;
import io.github.durablecps.store.AppliedSchemaMigration;
import io.github.durablecps.store.InMemoryWorkflowStore;
import io.github.durablecps.store.SchemaManagedWorkflowStore;
import io.github.durablecps.store.SchemaStatus;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class EngineLifecycleAndSchemaTest {
    @Test
    void handlersRunOffPollingAndCallerThreadsAndGracefulCloseDrains() throws Exception {
        WorkflowDefinition definition = new WorkflowDefinition("drain", 1, """
                def value = step('blocked', null)
                return value
                """);
        CompletableFuture<Serializable> result = new CompletableFuture<>();
        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<String> handlerThread = new AtomicReference<>();

        DurableWorkflowEngine engine = DurableWorkflowEngine.builder(new InMemoryWorkflowStore())
                .definition(definition)
                .step("blocked", (request, context) -> {
                    handlerThread.set(Thread.currentThread().getName());
                    started.countDown();
                    return result;
                })
                .automaticPolling(false)
                .shutdownTimeout(Duration.ofSeconds(2))
                .build();
        engine.start("drain-1", "drain", Map.of());
        assertTrue(started.await(2, TimeUnit.SECONDS));
        assertTrue(handlerThread.get().startsWith("durable-cps-handler-"));

        Thread completer = new Thread(() -> {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            result.complete("done");
        });
        completer.start();
        var shutdown = engine.closeGracefully(Duration.ofSeconds(2));
        completer.join();

        assertTrue(shutdown.drained());
        assertEquals(EngineLifecycleState.CLOSED, engine.lifecycle());
    }


    @Test
    void blockingHandlerEntryDoesNotStarveCompletionCallbacks() throws Exception {
        ExecutorService handlers = Executors.newSingleThreadExecutor();
        ScheduledExecutorService control = Executors.newSingleThreadScheduledExecutor();
        CountDownLatch blockerEntered = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        CompletableFuture<Serializable> asynchronous = new CompletableFuture<>();

        try (DurableWorkflowEngine engine = DurableWorkflowEngine.builder(new InMemoryWorkflowStore())
                .definition(new WorkflowDefinition("callback", 1, "return step('async', null)"))
                .definition(new WorkflowDefinition("blocker", 1, "return step('blocking', null)"))
                .executor(control)
                .handlerExecutor(handlers)
                .automaticPolling(false)
                .step("async", (request, context) -> asynchronous)
                .step("blocking", (request, context) -> {
                    blockerEntered.countDown();
                    try {
                        releaseBlocker.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return CompletableFuture.failedFuture(interrupted);
                    }
                    return CompletableFuture.completedFuture("released");
                })
                .build()) {
            String asynchronousId = engine.start("callback-1", "callback", Map.of());
            engine.start("blocker-1", "blocker", Map.of());
            assertTrue(blockerEntered.await(2, TimeUnit.SECONDS));

            asynchronous.complete("completed while handler entry was blocked");
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            while (engine.get(asynchronousId).orElseThrow().status()
                    != io.github.durablecps.api.WorkflowStatus.COMPLETED
                    && System.nanoTime() < deadline) {
                Thread.sleep(5L);
            }
            assertEquals(io.github.durablecps.api.WorkflowStatus.COMPLETED,
                    engine.get(asynchronousId).orElseThrow().status());
            releaseBlocker.countDown();
            assertTrue(engine.runUntilIdle(Duration.ofSeconds(2)));
        } finally {
            releaseBlocker.countDown();
            handlers.shutdownNow();
            control.shutdownNow();
        }
    }

    @Test
    void zeroTimeoutShutdownReportsDurableWorkForTakeover() throws Exception {
        CompletableFuture<Serializable> pending = new CompletableFuture<>();
        CountDownLatch entered = new CountDownLatch(1);
        DurableWorkflowEngine engine = DurableWorkflowEngine.builder(new InMemoryWorkflowStore())
                .definition(new WorkflowDefinition("timeout", 1, "return step('pending', null)"))
                .step("pending", (request, context) -> {
                    entered.countDown();
                    return pending;
                })
                .automaticPolling(false)
                .shutdownTimeout(Duration.ZERO)
                .build();
        engine.start("timeout-1", "timeout", Map.of());
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        var result = engine.closeGracefully(Duration.ZERO);
        assertFalse(result.drained());
        assertEquals(1, result.inFlightSteps());
        assertEquals(EngineLifecycleState.CLOSED, engine.lifecycle());
        pending.complete("late");
    }

    @Test
    void quiesceRejectsNewStartsAndResumeRestoresAdmission() {
        try (DurableWorkflowEngine engine = DurableWorkflowEngine.builder(new InMemoryWorkflowStore())
                .definition(new WorkflowDefinition("simple", 1, "return 'ok'"))
                .automaticPolling(false)
                .build()) {
            engine.quiesce();
            assertEquals(EngineLifecycleState.QUIESCING, engine.lifecycle());
            assertThrows(IllegalStateException.class, () -> engine.start("simple", Map.of()));
            assertEquals(io.github.durablecps.admin.HealthStatus.DEGRADED,
                    engine.administration().health().status());
            engine.resume();
            assertEquals(EngineLifecycleState.RUNNING, engine.lifecycle());
            engine.start("simple-1", "simple", Map.of());
        }
    }

    @Test
    void boundsConcurrentStepDispatch() throws Exception {
        WorkflowDefinition definition = new WorkflowDefinition("bounded", 1, "return step('gate', input('id'))");
        CompletableFuture<Serializable> first = new CompletableFuture<>();
        CompletableFuture<Serializable> second = new CompletableFuture<>();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);

        try (DurableWorkflowEngine engine = DurableWorkflowEngine.builder(new InMemoryWorkflowStore())
                .definition(definition)
                .maximumInFlightSteps(1)
                .automaticPolling(false)
                .step("gate", (request, context) -> {
                    if ("one".equals(request.argument())) {
                        firstStarted.countDown();
                        return first;
                    }
                    secondStarted.countDown();
                    return second;
                })
                .build()) {
            engine.start("bounded-1", "bounded", Map.of("id", "one"));
            engine.start("bounded-2", "bounded", Map.of("id", "two"));
            assertTrue(firstStarted.await(2, TimeUnit.SECONDS));
            assertFalse(secondStarted.await(100, TimeUnit.MILLISECONDS));
            first.complete("first");
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            while (secondStarted.getCount() > 0 && System.nanoTime() < deadline) {
                engine.tick();
                Thread.sleep(5L);
            }
            assertTrue(secondStarted.getCount() == 0);
            second.complete("second");
            assertTrue(engine.runUntilIdle(Duration.ofSeconds(2)));
        }
    }

    @Test
    void schemaPreflightCanFailOrMigrateBeforePolling() {
        FakeSchemaStore failing = new FakeSchemaStore(false);
        assertThrows(SchemaPreflightException.class, () -> DurableWorkflowEngine.builder(failing)
                .schemaPreflight(SchemaPreflightMode.FAIL)
                .automaticPolling(false)
                .build());

        FakeSchemaStore migrating = new FakeSchemaStore(false);
        try (DurableWorkflowEngine engine = DurableWorkflowEngine.builder(migrating)
                .schemaPreflight(SchemaPreflightMode.MIGRATE)
                .automaticPolling(false)
                .build()) {
            assertTrue(migrating.migrated.get());
            assertTrue(engine.administration().schemaStatus().orElseThrow().compatible());
        }
    }

    private static final class FakeSchemaStore implements SchemaManagedWorkflowStore {
        private final InMemoryWorkflowStore delegate = new InMemoryWorkflowStore();
        private final AtomicBoolean migrated = new AtomicBoolean();
        private volatile boolean compatible;

        private FakeSchemaStore(boolean compatible) {
            this.compatible = compatible;
        }

        @Override
        public SchemaStatus schemaStatus() {
            return compatible
                    ? new SchemaStatus(3, 3, true,
                            List.of(new AppliedSchemaMigration(3, "test", "checksum", Instant.now())),
                            List.of(), "current")
                    : new SchemaStatus(0, 3, false, List.of(), List.of(1, 2, 3), "migration required");
        }

        @Override
        public SchemaStatus migrateSchema() {
            migrated.set(true);
            compatible = true;
            return schemaStatus();
        }

        @Override public WorkflowRecord create(WorkflowRecord initial) { return delegate.create(initial); }
        @Override public Optional<WorkflowRecord> load(String workflowId) { return delegate.load(workflowId); }
        @Override public WorkflowRecord replace(String workflowId, long expectedRevision, WorkflowRecord replacement) {
            return delegate.replace(workflowId, expectedRevision, replacement);
        }
        @Override public Collection<WorkflowRecord> list() { return delegate.list(); }
    }
}
