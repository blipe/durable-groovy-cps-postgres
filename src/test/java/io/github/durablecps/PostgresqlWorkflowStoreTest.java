package io.github.durablecps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.durablecps.admin.ArchiveRequest;
import io.github.durablecps.api.WorkflowDefinition;
import io.github.durablecps.api.WorkflowStatus;
import io.github.durablecps.runtime.DurableWorkflowEngine;
import io.github.durablecps.runtime.Failpoint;
import io.github.durablecps.store.JdbcIdempotencyStore;
import io.github.durablecps.store.JdbcWorkflowStore;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers(disabledWithoutDocker = true)
class PostgresqlWorkflowStoreTest {
    @Container
    private static final PostgreSQLContainer postgres =
            new PostgreSQLContainer("postgres:17-alpine");


    @Test
    void migrationsAreChecksummedVersionedAndIdempotent() throws Exception {
        JdbcWorkflowStore store = new JdbcWorkflowStore(dataSource());
        var first = store.migrateSchema();
        assertTrue(first.compatible());
        assertEquals(3, first.currentVersion());
        assertEquals(3, first.applied().size());
        assertTrue(first.pendingVersions().isEmpty());

        var second = store.migrateSchema();
        assertEquals(first.currentVersion(), second.currentVersion());
        assertEquals(first.applied().stream().map(item -> item.checksum()).toList(),
                second.applied().stream().map(item -> item.checksum()).toList());

        try (Connection connection = dataSource().getConnection();
                java.sql.Statement statement = connection.createStatement();
                java.sql.ResultSet rows = statement.executeQuery(
                        "select count(*) from durable_schema_history")) {
            rows.next();
            assertEquals(3L, rows.getLong(1));
        }
    }

    @Test
    void migratedPostgresqlSchemaArchivesAndReloadsCompletedWorkflow() {
        DataSource dataSource = dataSource();
        JdbcWorkflowStore store = new JdbcWorkflowStore(dataSource);
        store.migrate();
        WorkflowDefinition definition = new WorkflowDefinition("archive-postgres", 1, "return 'done'");
        try (DurableWorkflowEngine engine = DurableWorkflowEngine.builder(store)
                .automaticPolling(false)
                .definition(definition)
                .build()) {
            engine.start("archive-postgres-1", "archive-postgres", Map.of());
            assertEquals(WorkflowStatus.COMPLETED, engine.get("archive-postgres-1").orElseThrow().status());
            var result = engine.administration().archive(
                    ArchiveRequest.completedBefore(Instant.now().plusSeconds(1)));
            var summary = result.archived().stream()
                    .filter(item -> item.workflowId().equals("archive-postgres-1"))
                    .findFirst()
                    .orElseThrow();
            var archived = engine.administration().archived(summary.archiveId()).orElseThrow();
            assertEquals("done", archived.workflow().output());
            assertTrue(engine.get("archive-postgres-1").isEmpty());
        }
    }

    @Test
    void jdbcIdempotencyCommitsBusinessEffectAndResultOnce() throws Exception {
        DataSource dataSource = dataSource();
        JdbcWorkflowStore schema = new JdbcWorkflowStore(dataSource);
        schema.migrate();
        try (Connection connection = dataSource.getConnection();
                java.sql.Statement statement = connection.createStatement()) {
            statement.execute("create table business_effect (effect_id varchar(100) primary key, value varchar(100))");
        }

        JdbcIdempotencyStore idempotency = new JdbcIdempotencyStore(dataSource);
        AtomicInteger callbacks = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            java.util.concurrent.Callable<String> call = () -> {
                start.await();
                return idempotency.executeOnce("workflow-9:3", String.class, connection -> {
                    callbacks.incrementAndGet();
                    try (java.sql.PreparedStatement insert = connection.prepareStatement(
                            "insert into business_effect(effect_id, value) values (?, ?)")) {
                        insert.setString(1, "effect-1");
                        insert.setString(2, "created");
                        insert.executeUpdate();
                    }
                    Thread.sleep(100L);
                    return "receipt-1";
                });
            };
            Future<String> first = pool.submit(call);
            Future<String> second = pool.submit(call);
            start.countDown();
            assertEquals("receipt-1", first.get());
            assertEquals("receipt-1", second.get());
        } finally {
            pool.shutdownNow();
        }

        assertEquals(1, callbacks.get());
        try (Connection connection = dataSource.getConnection();
                java.sql.Statement statement = connection.createStatement();
                java.sql.ResultSet rows = statement.executeQuery("select count(*) from business_effect")) {
            rows.next();
            assertEquals(1L, rows.getLong(1));
        }
    }

    @Test
    void twoNodesCannotExecuteSameLiveStepConcurrently() throws Exception {
        DataSource dataSource = dataSource();
        JdbcWorkflowStore storeA = new JdbcWorkflowStore(dataSource);
        storeA.migrate();
        JdbcWorkflowStore storeB = new JdbcWorkflowStore(dataSource);
        WorkflowDefinition definition = new WorkflowDefinition(
                "single-step", 1, "return step('once', input('value'))");
        AtomicInteger executions = new AtomicInteger();
        CompletableFuture<String> gate = new CompletableFuture<>();
        CopyOnWriteArrayList<String> invocationIds = new CopyOnWriteArrayList<>();

        try (DurableWorkflowEngine engineA = DurableWorkflowEngine.builder(storeA)
                        .instanceId("node-a")
                        .automaticPolling(false)
                        .definition(definition)
                        .step("once", (request, context) -> {
                            executions.incrementAndGet();
                            invocationIds.add(request.invocationId());
                            return gate;
                        })
                        .build();
                DurableWorkflowEngine engineB = DurableWorkflowEngine.builder(storeB)
                        .instanceId("node-b")
                        .automaticPolling(false)
                        .definition(definition)
                        .step("once", (request, context) -> {
                            executions.incrementAndGet();
                            invocationIds.add(request.invocationId());
                            return gate;
                        })
                        .build()) {
            engineA.start("two-node", "single-step", Map.of("value", "ok"));
            await(() -> executions.get() == 1, Duration.ofSeconds(3));
            for (int i = 0; i < 10; i++) engineB.tick();
            assertEquals(1, executions.get());

            gate.complete("ok");
            assertTrue(engineA.runUntilIdle(Duration.ofSeconds(5)));
            assertEquals(WorkflowStatus.COMPLETED, engineA.get("two-node").orElseThrow().status());
            assertEquals(1, executions.get());
            assertEquals(1, invocationIds.stream().distinct().count());
        }
    }

    @Test
    void globalStepConcurrencyLimitAppliesAcrossDifferentWorkflowsAndNodes() throws Exception {
        DataSource dataSource = dataSource();
        JdbcWorkflowStore storeA = new JdbcWorkflowStore(dataSource);
        storeA.migrate();
        JdbcWorkflowStore storeB = new JdbcWorkflowStore(dataSource);
        WorkflowDefinition definition = new WorkflowDefinition(
                "shared-capacity", 1, "return step('shared-api', input('value'))");
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maximumObserved = new AtomicInteger();
        CopyOnWriteArrayList<CompletableFuture<String>> gates = new CopyOnWriteArrayList<>();

        io.github.durablecps.api.StepHandler handler = (request, context) -> {
            int current = running.incrementAndGet();
            maximumObserved.accumulateAndGet(current, Math::max);
            CompletableFuture<String> gate = new CompletableFuture<>();
            gates.add(gate);
            return gate.whenComplete((value, failure) -> running.decrementAndGet());
        };

        try (DurableWorkflowEngine engineA = DurableWorkflowEngine.builder(storeA)
                        .instanceId("capacity-a")
                        .automaticPolling(false)
                        .definition(definition)
                        .step("shared-api", handler)
                        .stepConcurrencyLimit("shared-api", 1)
                        .build();
                DurableWorkflowEngine engineB = DurableWorkflowEngine.builder(storeB)
                        .instanceId("capacity-b")
                        .automaticPolling(false)
                        .definition(definition)
                        .step("shared-api", handler)
                        .stepConcurrencyLimit("shared-api", 1)
                        .build()) {
            engineA.start("capacity-1", "shared-capacity", Map.of("value", "one"));
            engineB.start("capacity-2", "shared-capacity", Map.of("value", "two"));
            await(() -> gates.size() == 1, Duration.ofSeconds(3));
            for (int i = 0; i < 20; i++) {
                engineA.tick();
                engineB.tick();
            }
            assertEquals(1, gates.size());
            assertEquals(1, maximumObserved.get());

            gates.get(0).complete("first");
            await(() -> {
                engineA.tick();
                engineB.tick();
                return gates.size() == 2;
            }, Duration.ofSeconds(5));
            gates.get(1).complete("second");
            assertTrue(engineA.runUntilIdle(Duration.ofSeconds(5)));
            assertTrue(engineB.runUntilIdle(Duration.ofSeconds(5)));
            assertEquals(1, maximumObserved.get());
            assertEquals(WorkflowStatus.COMPLETED, engineA.get("capacity-1").orElseThrow().status());
            assertEquals(WorkflowStatus.COMPLETED, engineB.get("capacity-2").orElseThrow().status());
        }
    }

    @Test
    void crashAfterAtomicSuspensionRecoversOnAnotherNode() {
        DataSource dataSource = dataSource();
        JdbcWorkflowStore firstStore = new JdbcWorkflowStore(dataSource);
        firstStore.migrate();
        WorkflowDefinition definition = new WorkflowDefinition(
                "crash-step", 1, "return step('work', input('value'))");
        AtomicInteger executions = new AtomicInteger();

        try (DurableWorkflowEngine first = DurableWorkflowEngine.builder(firstStore)
                .instanceId("crashing-node")
                .automaticPolling(false)
                .definition(definition)
                .step("work", (request, context) -> {
                    executions.incrementAndGet();
                    return CompletableFuture.completedFuture("ok");
                })
                .failureInjector((point, workflowId, operationId) -> {
                    if (point == Failpoint.AFTER_WORKFLOW_SUSPENDED) throw new SimulatedCrash();
                })
                .build()) {
            try {
                first.start("crash-window", "crash-step", Map.of("value", "x"));
            } catch (SimulatedCrash expected) {
                // Continuation and step row were committed in one transaction.
            }
        }

        try (DurableWorkflowEngine recovered = DurableWorkflowEngine.builder(new JdbcWorkflowStore(dataSource))
                .instanceId("recovery-node")
                .automaticPolling(false)
                .definition(definition)
                .step("work", (request, context) -> {
                    executions.incrementAndGet();
                    return CompletableFuture.completedFuture("ok");
                })
                .build()) {
            assertTrue(recovered.runUntilIdle(Duration.ofSeconds(5)));
            assertEquals(
                    WorkflowStatus.COMPLETED,
                    recovered.get("crash-window").orElseThrow().status());
            assertEquals(1, executions.get());
        }
    }


    @Test
    void crashAfterHandlerBeforeResultCausesAtLeastOnceRetryWithSameInvocationId() throws Exception {
        DataSource dataSource = dataSource();
        JdbcWorkflowStore firstStore = new JdbcWorkflowStore(dataSource);
        firstStore.migrate();
        WorkflowDefinition definition = new WorkflowDefinition(
                "duplicate-window", 1, "return step('effect', input('value'))");
        AtomicInteger executions = new AtomicInteger();
        CopyOnWriteArrayList<String> invocationIds = new CopyOnWriteArrayList<>();

        try (DurableWorkflowEngine first = DurableWorkflowEngine.builder(firstStore)
                .instanceId("first-effect-node")
                .automaticPolling(false)
                .stepLease(Duration.ofMillis(150))
                .definition(definition)
                .step("effect", (request, context) -> {
                    executions.incrementAndGet();
                    invocationIds.add(request.invocationId());
                    return CompletableFuture.completedFuture("ok");
                })
                .failureInjector((point, workflowId, operationId) -> {
                    if (point == Failpoint.AFTER_STEP_HANDLER) throw new SimulatedCrash();
                })
                .build()) {
            first.start("duplicate-crash", "duplicate-window", Map.of("value", "x"));
            await(() -> executions.get() == 1, Duration.ofSeconds(3));
            await(() -> firstStore.loadStep("duplicate-crash:1")
                            .map(step -> step.status().name().equals("RUNNING"))
                            .orElse(false),
                    Duration.ofSeconds(3));
        }

        Thread.sleep(200L);
        try (DurableWorkflowEngine recovered = DurableWorkflowEngine.builder(new JdbcWorkflowStore(dataSource))
                .instanceId("second-effect-node")
                .automaticPolling(false)
                .stepLease(Duration.ofMillis(150))
                .definition(definition)
                .step("effect", (request, context) -> {
                    executions.incrementAndGet();
                    invocationIds.add(request.invocationId());
                    return CompletableFuture.completedFuture("ok");
                })
                .build()) {
            assertTrue(recovered.runUntilIdle(Duration.ofSeconds(5)));
            assertEquals(
                    WorkflowStatus.COMPLETED,
                    recovered.get("duplicate-crash").orElseThrow().status());
            assertEquals(2, executions.get());
            assertEquals(1, invocationIds.stream().distinct().count());
        }
    }

    @Test
    void crashAfterResultCommitDoesNotReexecuteStep() throws Exception {
        DataSource dataSource = dataSource();
        JdbcWorkflowStore firstStore = new JdbcWorkflowStore(dataSource);
        firstStore.migrate();
        WorkflowDefinition definition = new WorkflowDefinition(
                "result-commit", 1, "return step('effect', input('value'))");
        AtomicInteger executions = new AtomicInteger();

        try (DurableWorkflowEngine first = DurableWorkflowEngine.builder(firstStore)
                .instanceId("result-node-a")
                .automaticPolling(false)
                .definition(definition)
                .step("effect", (request, context) -> {
                    executions.incrementAndGet();
                    return CompletableFuture.completedFuture("ok");
                })
                .failureInjector((point, workflowId, operationId) -> {
                    if (point == Failpoint.AFTER_STEP_RESULT_STORED) throw new SimulatedCrash();
                })
                .build()) {
            first.start("result-crash", "result-commit", Map.of("value", "x"));
            await(() -> first.get("result-crash")
                            .map(view -> view.status() == WorkflowStatus.READY)
                            .orElse(false),
                    Duration.ofSeconds(3));
        }

        try (DurableWorkflowEngine recovered = DurableWorkflowEngine.builder(new JdbcWorkflowStore(dataSource))
                .instanceId("result-node-b")
                .automaticPolling(false)
                .definition(definition)
                .step("effect", (request, context) -> {
                    executions.incrementAndGet();
                    return CompletableFuture.completedFuture("unexpected");
                })
                .build()) {
            assertTrue(recovered.runUntilIdle(Duration.ofSeconds(5)));
            assertEquals(
                    WorkflowStatus.COMPLETED,
                    recovered.get("result-crash").orElseThrow().status());
            assertEquals(1, executions.get());
        }
    }

    private static void await(java.util.function.BooleanSupplier condition, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(condition.getAsBoolean(), "condition was not met before timeout");
    }

    private static DataSource dataSource() {
        return new DriverManagerDataSource(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private static final class SimulatedCrash extends Error {
        @java.io.Serial private static final long serialVersionUID = 1L;
    }

    private record DriverManagerDataSource(String url, String username, String password)
            implements DataSource {
        @Override
        public Connection getConnection() throws SQLException {
            return DriverManager.getConnection(url, username, password);
        }

        @Override
        public Connection getConnection(String user, String pass) throws SQLException {
            return DriverManager.getConnection(url, user, pass);
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return DriverManager.getLogWriter();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            DriverManager.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            DriverManager.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return DriverManager.getLoginTimeout();
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getLogger("global");
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            if (iface.isInstance(this)) return iface.cast(this);
            throw new SQLException("not a wrapper for " + iface.getName());
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return iface.isInstance(this);
        }
    }
}
