package io.github.durablecps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.durablecps.admin.HealthStatus;
import io.github.durablecps.admin.WorkflowQuery;
import io.github.durablecps.api.WorkflowDefinition;
import io.github.durablecps.api.WorkflowStatus;
import io.github.durablecps.observability.WorkflowContextPropagator;
import io.github.durablecps.observability.WorkflowEvent;
import io.github.durablecps.observability.WorkflowEventType;
import io.github.durablecps.observability.WorkflowListener;
import io.github.durablecps.runtime.DurableWorkflowEngine;
import io.github.durablecps.store.FileWorkflowStore;
import io.github.durablecps.store.InMemoryWorkflowStore;
import java.io.Serializable;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DurableWorkflowEngineTest {
    @TempDir Path temporaryDirectory;

    @Test
    void runsStepAndCompletes() {
        WorkflowDefinition workflow = new WorkflowDefinition("hello", 1, """
                def greeting = step('greet', input('name'))
                return [greeting: greeting, count: 1]
                """);

        try (DurableWorkflowEngine engine = DurableWorkflowEngine.builder(new InMemoryWorkflowStore())
                .definition(workflow)
                .step("greet", (request, context) ->
                        CompletableFuture.completedFuture("hello " + request.argument()))
                .automaticPolling(false)
                .build()) {
            String id = engine.start("hello", Map.of("name", "Ada"));
            assertTrue(engine.runUntilIdle(Duration.ofSeconds(2)));
            var view = engine.get(id).orElseThrow();
            assertEquals(WorkflowStatus.COMPLETED, view.status());
            assertEquals(Map.of("greeting", "hello Ada", "count", 1), view.output());
        }
    }

    @Test
    void resumesPendingStepAfterProcessRestart() {
        Path state = temporaryDirectory.resolve("state");
        WorkflowDefinition workflow = new WorkflowDefinition("restart", 1, """
                def value = step('external', 'request')
                return "done:${value}"
                """);
        CompletableFuture<Serializable> neverCompletes = new CompletableFuture<>();
        String id;

        try (DurableWorkflowEngine first = DurableWorkflowEngine.builder(new FileWorkflowStore(state))
                .definition(workflow)
                .step("external", (request, context) -> neverCompletes)
                .automaticPolling(false)
                .shutdownTimeout(Duration.ZERO)
                .build()) {
            id = first.start("restart", Map.of());
            assertEquals(WorkflowStatus.WAITING, first.get(id).orElseThrow().status());
        }

        try (DurableWorkflowEngine second = DurableWorkflowEngine.builder(new FileWorkflowStore(state))
                .definition(workflow)
                .step("external", (request, context) -> CompletableFuture.completedFuture("recovered"))
                .automaticPolling(false)
                .build()) {
            second.tick();
            assertTrue(second.runUntilIdle(Duration.ofSeconds(2)));
            var view = second.get(id).orElseThrow();
            assertEquals(WorkflowStatus.COMPLETED, view.status());
            assertEquals("done:recovered", view.output());
        }
    }

    @Test
    void queuesEarlySignalAndConsumesItLater() {
        WorkflowDefinition workflow = new WorkflowDefinition("signal", 1, """
                def before = step('gate', null)
                def approval = awaitSignal('approve')
                return [before: before, approval: approval]
                """);
        CompletableFuture<Serializable> gate = new CompletableFuture<>();

        try (DurableWorkflowEngine engine = DurableWorkflowEngine.builder(new InMemoryWorkflowStore())
                .definition(workflow)
                .step("gate", (request, context) -> gate)
                .automaticPolling(false)
                .build()) {
            String id = engine.start("signal", Map.of());
            engine.signal(id, "approve", "yes");
            gate.complete("opened");
            assertTrue(engine.runUntilIdle(Duration.ofSeconds(2)));
            var view = engine.get(id).orElseThrow();
            assertEquals(WorkflowStatus.COMPLETED, view.status());
            assertEquals(Map.of("before", "opened", "approval", "yes"), view.output());
        }
    }

    @Test
    void retriesStepWithStableInvocationId() {
        WorkflowDefinition workflow = new WorkflowDefinition("retry", 1, """
                def options = io.github.durablecps.api.StepOptions.retry(3, java.time.Duration.ZERO)
                return step('flaky', null, options)
                """);
        AtomicInteger attempts = new AtomicInteger();
        java.util.Set<String> invocationIds = java.util.concurrent.ConcurrentHashMap.newKeySet();

        try (DurableWorkflowEngine engine = DurableWorkflowEngine.builder(new InMemoryWorkflowStore())
                .definition(workflow)
                .step("flaky", (request, context) -> {
                    invocationIds.add(request.invocationId());
                    if (attempts.incrementAndGet() < 3) {
                        return CompletableFuture.<Serializable>failedFuture(new IllegalStateException("not yet"));
                    }
                    return CompletableFuture.completedFuture("ok");
                })
                .automaticPolling(false)
                .build()) {
            String id = engine.start("retry", Map.of());
            assertTrue(engine.runUntilIdle(Duration.ofSeconds(2)));
            assertEquals("ok", engine.get(id).orElseThrow().output());
            assertEquals(3, attempts.get());
            assertEquals(1, invocationIds.size());
        }
    }

    @Test
    void deadLettersMissingDefinitionAndRecoversWhenExactVersionReturns() {
        Path state = temporaryDirectory.resolve("version-state");
        WorkflowDefinition versionOne = new WorkflowDefinition("versioned", 1, "return awaitSignal('go')");
        String id;
        try (DurableWorkflowEngine first = DurableWorkflowEngine.builder(new FileWorkflowStore(state))
                .definition(versionOne)
                .automaticPolling(false)
                .build()) {
            id = first.start("versioned", Map.of());
        }

        WorkflowDefinition versionTwo = new WorkflowDefinition("versioned", 2, "return 'changed'");
        try (DurableWorkflowEngine second = DurableWorkflowEngine.builder(new FileWorkflowStore(state))
                .definition(versionTwo)
                .automaticPolling(false)
                .build()) {
            second.signal(id, "go", "continue");
            var dead = second.get(id).orElseThrow();
            assertEquals(WorkflowStatus.DEAD_LETTERED, dead.status());
            assertTrue(dead.failureType().contains("DefinitionMismatchException"));

            second.registerDefinition(versionOne);
            second.administration().retry(id, "restored exact workflow definition");
            assertTrue(second.runUntilIdle(Duration.ofSeconds(2)));
            assertEquals("continue", second.get(id).orElseThrow().output());
        }
    }
    @Test
    void keepsOldVersionForResumeAndStartsLatestVersion() {
        Path state = temporaryDirectory.resolve("multi-version-state");
        WorkflowDefinition versionOne = new WorkflowDefinition("multi", 1, "return awaitSignal('go')");
        WorkflowDefinition versionTwo = new WorkflowDefinition("multi", 2, "return 'v2'");
        String oldId;

        try (DurableWorkflowEngine first = DurableWorkflowEngine.builder(new FileWorkflowStore(state))
                .definition(versionOne)
                .automaticPolling(false)
                .build()) {
            oldId = first.start("old", "multi", Map.of());
        }

        try (DurableWorkflowEngine second = DurableWorkflowEngine.builder(new FileWorkflowStore(state))
                .definition(versionOne)
                .definition(versionTwo)
                .automaticPolling(false)
                .build()) {
            second.signal(oldId, "go", "v1-resumed");
            assertTrue(second.runUntilIdle(Duration.ofSeconds(2)));
            assertEquals("v1-resumed", second.get(oldId).orElseThrow().output());

            String newId = second.start("new", "multi", Map.of());
            assertTrue(second.runUntilIdle(Duration.ofSeconds(2)));
            assertEquals("v2", second.get(newId).orElseThrow().output());
            assertEquals(2, second.get(newId).orElseThrow().definitionVersion());
        }
    }

    @Test
    void deadLettersExhaustedStepAndAllowsAdministrativeRetry() {
        WorkflowDefinition workflow = new WorkflowDefinition("operations", 1, """
                def options = io.github.durablecps.api.StepOptions.retry(1, java.time.Duration.ZERO)
                return step('flaky-once', null, options)
                """);
        AtomicInteger attempts = new AtomicInteger();
        CopyOnWriteArrayList<WorkflowEventType> events = new CopyOnWriteArrayList<>();

        try (DurableWorkflowEngine engine = DurableWorkflowEngine.builder(new InMemoryWorkflowStore())
                .definition(workflow)
                .step("flaky-once", (request, context) -> attempts.incrementAndGet() == 1
                        ? CompletableFuture.<Serializable>failedFuture(new IllegalStateException("first failure"))
                        : CompletableFuture.completedFuture("recovered"))
                .listener(new WorkflowListener() {
                    @Override public void onWorkflow(WorkflowEvent event) { events.add(event.type()); }
                })
                .automaticPolling(false)
                .build()) {
            String id = engine.start("operations", Map.of());
            assertTrue(engine.runUntilIdle(Duration.ofSeconds(2)));

            var dead = engine.get(id).orElseThrow();
            assertEquals(WorkflowStatus.DEAD_LETTERED, dead.status());
            assertTrue(dead.recoverable());
            assertEquals(HealthStatus.DEGRADED, engine.administration().health().status());
            assertEquals(1L, engine.administration()
                    .search(WorkflowQuery.all().withStatuses(Set.of(WorkflowStatus.DEAD_LETTERED)))
                    .total());
            assertTrue(events.contains(WorkflowEventType.DEAD_LETTERED));

            engine.administration().retry(id, "operator approved retry");
            assertTrue(engine.runUntilIdle(Duration.ofSeconds(2)));
            assertEquals(WorkflowStatus.COMPLETED, engine.get(id).orElseThrow().status());
            assertEquals("recovered", engine.get(id).orElseThrow().output());
            assertEquals(2, attempts.get());
            assertEquals(HealthStatus.UP, engine.administration().health().status());
            assertTrue(events.contains(WorkflowEventType.RECOVERED));
        }
    }

    @Test
    void capturesAndRestoresWorkflowTelemetryContext() {
        WorkflowDefinition workflow = new WorkflowDefinition("context", 1, "return step('echo', 'ok')");
        AtomicInteger restores = new AtomicInteger();
        WorkflowContextPropagator propagator = new WorkflowContextPropagator() {
            @Override public Map<String, String> capture() { return Map.of("traceparent", "test-trace"); }
            @Override public Scope restore(Map<String, String> context) {
                assertEquals("test-trace", context.get("traceparent"));
                restores.incrementAndGet();
                return Scope.NONE;
            }
        };

        try (DurableWorkflowEngine engine = DurableWorkflowEngine.builder(new InMemoryWorkflowStore())
                .definition(workflow)
                .step("echo", (request, context) -> CompletableFuture.completedFuture(request.argument()))
                .contextPropagator(propagator)
                .automaticPolling(false)
                .build()) {
            String id = engine.start("context", Map.of());
            assertTrue(engine.runUntilIdle(Duration.ofSeconds(2)));
            assertEquals("ok", engine.get(id).orElseThrow().output());
            assertTrue(restores.get() >= 2, "context should be restored for CPS execution and step handling");
        }
    }

}
