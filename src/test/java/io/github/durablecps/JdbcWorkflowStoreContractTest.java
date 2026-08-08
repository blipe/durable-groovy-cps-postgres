package io.github.durablecps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.durablecps.admin.ArchiveRequest;
import io.github.durablecps.admin.ArchiveResult;
import io.github.durablecps.admin.WorkflowQuery;
import io.github.durablecps.api.DurableCommand;
import io.github.durablecps.api.StepOptions;
import io.github.durablecps.api.WorkflowStatus;
import io.github.durablecps.runtime.DurableWorkflowEngine;
import io.github.durablecps.runtime.state.HistoryEntry;
import io.github.durablecps.runtime.state.ResumeValue;
import io.github.durablecps.runtime.state.SignalEnvelope;
import io.github.durablecps.runtime.state.WorkflowRecord;
import io.github.durablecps.store.ClaimedStep;
import io.github.durablecps.store.JdbcWorkflowStore;
import io.github.durablecps.store.LeaseLostException;
import io.github.durablecps.store.StepInvocationRecord;
import io.github.durablecps.store.StepInvocationStatus;
import io.github.durablecps.store.WorkflowLease;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcWorkflowStoreContractTest {
    private JDBCDataSource dataSource;
    private JdbcWorkflowStore store;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new JDBCDataSource();
        dataSource.setUrl("jdbc:hsqldb:mem:contract-" + System.nanoTime());
        dataSource.setUser("sa");
        dataSource.setPassword("");
        createPortableSchema();
        store = new JdbcWorkflowStore(dataSource);
    }

    @Test
    void transactionalStepLifecycleAndFencing() {
        Instant now = Instant.now();
        WorkflowRecord initial = ready("workflow-1", now);
        store.create(initial);
        assertEquals("early-value", store.load(initial.id()).orElseThrow().signals().get(0).payload());

        WorkflowLease workflowLease = store.tryAcquireWorkflowLease(
                        initial.id(), "node-a", Duration.ofSeconds(10))
                .orElseThrow();
        assertFalse(store.tryAcquireWorkflowLease(
                        initial.id(), "node-b", Duration.ofSeconds(10))
                .isPresent());

        DurableCommand.Step command = new DurableCommand.Step(
                1L,
                "charge",
                "argument",
                StepOptions.retry(3, Duration.ofMillis(5), Duration.ofSeconds(10)));
        WorkflowRecord current = store.load(initial.id()).orElseThrow();
        WorkflowRecord waiting = current.next(
                WorkflowStatus.WAITING,
                current.continuation(),
                command,
                null,
                null,
                null,
                0,
                now,
                current.signals(),
                new HistoryEntry(now, "SUSPENDED", "charge"),
                now);
        StepInvocationRecord pending = StepInvocationRecord.pending(
                "workflow-1:1",
                "workflow-1",
                1L,
                "charge",
                "argument",
                command.options(),
                now);
        store.suspendWithStep(workflowLease, current.revision(), waiting, pending);
        store.releaseWorkflowLease(workflowLease);

        ClaimedStep claimed = store.tryClaimStep(
                        pending.invocationId(), "worker-a", Duration.ofSeconds(10))
                .orElseThrow();
        assertEquals(1, claimed.invocation().attempt());
        assertFalse(store.tryClaimStep(
                        pending.invocationId(), "worker-b", Duration.ofSeconds(10))
                .isPresent());

        current = store.load(initial.id()).orElseThrow();
        WorkflowRecord resumed = current.next(
                WorkflowStatus.READY,
                current.continuation(),
                null,
                new ResumeValue.Success("approved"),
                null,
                null,
                1,
                now,
                current.signals(),
                new HistoryEntry(now, "STEP_COMPLETED", pending.invocationId()),
                now);
        store.completeStep(
                claimed.lease(),
                current.revision(),
                resumed,
                claimed.invocation().succeeded("approved", now));

        assertEquals(WorkflowStatus.READY, store.load(initial.id()).orElseThrow().status());
        assertEquals(
                StepInvocationStatus.SUCCEEDED,
                store.loadStep(pending.invocationId()).orElseThrow().status());

        WorkflowRecord completed = resumed.next(
                WorkflowStatus.COMPLETED,
                null,
                null,
                null,
                "done",
                null,
                1,
                null,
                resumed.signals(),
                new HistoryEntry(now, "COMPLETED", "done"),
                now);
        assertThrows(
                LeaseLostException.class,
                () -> store.completeStep(
                        claimed.lease(),
                        resumed.revision(),
                        completed,
                        claimed.invocation().succeeded("approved", now)));
    }

    @Test
    void expiredWorkflowLeaseGetsHigherFencingToken() throws Exception {
        WorkflowRecord initial = ready("workflow-2", Instant.now());
        store.create(initial);
        WorkflowLease stale = store.tryAcquireWorkflowLease(
                        initial.id(), "node-a", Duration.ofMillis(40))
                .orElseThrow();

        WorkflowLease current = null;
        long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (current == null && System.nanoTime() < deadline) {
            current = store.tryAcquireWorkflowLease(
                            initial.id(), "node-b", Duration.ofSeconds(5))
                    .orElse(null);
            if (current == null) Thread.sleep(10L);
        }
        assertTrue(current != null, "second node should acquire expired lease");
        assertTrue(current.token() > stale.token());

        WorkflowRecord record = store.load(initial.id()).orElseThrow();
        WorkflowRecord replacement = record.next(
                WorkflowStatus.COMPLETED,
                null,
                null,
                null,
                "new-owner",
                null,
                0,
                null,
                record.signals(),
                new HistoryEntry(Instant.now(), "COMPLETED", "new-owner"),
                Instant.now());
        assertThrows(
                LeaseLostException.class,
                () -> store.replaceWithLease(stale, record.revision(), replacement));
        store.replaceWithLease(current, record.revision(), replacement);
    }



    @Test
    void engineAdministrationCancelsAndRequeuesPendingJdbcStepAtomically() {
        Instant now = Instant.now();
        WorkflowRecord initial = ready("admin-atomic", now);
        store.create(initial);
        WorkflowLease lease = store.tryAcquireWorkflowLease(
                        initial.id(), "node-a", Duration.ofSeconds(10))
                .orElseThrow();
        DurableCommand.Step command = new DurableCommand.Step(
                1L, "work", "argument", StepOptions.retry(2, Duration.ZERO));
        WorkflowRecord waiting = initial.next(
                WorkflowStatus.WAITING,
                initial.continuation(),
                command,
                null,
                null,
                null,
                0,
                now,
                initial.signals(),
                new HistoryEntry(now, "SUSPENDED", "work"),
                now);
        StepInvocationRecord step = StepInvocationRecord.pending(
                "admin-atomic:1", "admin-atomic", 1L, "work", "argument", command.options(), now);
        store.suspendWithStep(lease, initial.revision(), waiting, step);
        store.releaseWorkflowLease(lease);

        try (DurableWorkflowEngine engine = DurableWorkflowEngine.builder(store)
                .automaticPolling(false)
                .build()) {
            engine.administration().deadLetter(initial.id(), "operator quarantine");
            assertEquals(WorkflowStatus.DEAD_LETTERED, engine.get(initial.id()).orElseThrow().status());
            assertEquals(StepInvocationStatus.CANCELLED, store.loadStep(step.invocationId()).orElseThrow().status());

            engine.administration().retry(initial.id(), "dependency restored");
            assertEquals(WorkflowStatus.WAITING, engine.get(initial.id()).orElseThrow().status());
            assertEquals(StepInvocationStatus.PENDING, store.loadStep(step.invocationId()).orElseThrow().status());
        }
    }

    @Test
    void supportsOperationalSearchHealthRecoveryAndCancellation() {
        Instant now = Instant.now();
        WorkflowRecord initial = ready("operations-1", now);
        store.create(initial);
        store.checkHealth();
        assertEquals(1L, store.countByStatus().get(WorkflowStatus.READY).longValue());
        assertEquals(1L, store.search(WorkflowQuery.all()
                .withStatuses(java.util.Set.of(WorkflowStatus.READY))
                .withDefinition("definition")).total());

        WorkflowLease lease = store.tryAcquireWorkflowLease(
                        initial.id(), "node-a", Duration.ofSeconds(10))
                .orElseThrow();
        DurableCommand.Step command = new DurableCommand.Step(
                1L, "work", "argument", StepOptions.retry(1, Duration.ZERO));
        WorkflowRecord current = store.load(initial.id()).orElseThrow();
        WorkflowRecord waiting = current.next(
                WorkflowStatus.WAITING,
                current.continuation(),
                command,
                null,
                null,
                null,
                0,
                now,
                current.signals(),
                new HistoryEntry(now, "SUSPENDED", "work"),
                now);
        StepInvocationRecord step = StepInvocationRecord.pending(
                "operations-1:1", "operations-1", 1L, "work", "argument", command.options(), now);
        store.suspendWithStep(lease, current.revision(), waiting, step);
        store.releaseWorkflowLease(lease);

        WorkflowRecord dead = waiting.next(
                WorkflowStatus.DEAD_LETTERED,
                waiting.continuation(),
                waiting.pendingCommand(),
                null,
                null,
                new io.github.durablecps.runtime.state.FailureData("failure", "exhausted", ""),
                1,
                now,
                waiting.signals(),
                new HistoryEntry(now, "DEAD_LETTERED", "exhausted"),
                now);
        StepInvocationRecord failed = step.running(1, now, now.plusSeconds(10))
                .failed(new io.github.durablecps.runtime.state.FailureData("failure", "exhausted", ""), now);
        store.recoverStep(waiting.revision(), dead, failed);

        WorkflowRecord recovered = dead.next(
                WorkflowStatus.WAITING,
                dead.continuation(),
                dead.pendingCommand(),
                null,
                null,
                null,
                0,
                now,
                dead.signals(),
                new HistoryEntry(now, "STEP_RECOVERED", "operator retry"),
                now);
        store.recoverStep(dead.revision(), recovered, failed.requeued(now));
        assertEquals(WorkflowStatus.WAITING, store.load(initial.id()).orElseThrow().status());
        assertEquals(StepInvocationStatus.PENDING, store.loadStep(step.invocationId()).orElseThrow().status());

        WorkflowRecord cancelled = recovered.next(
                WorkflowStatus.CANCELLED,
                null,
                null,
                null,
                null,
                null,
                0,
                null,
                recovered.signals(),
                new HistoryEntry(now, "CANCELLED", "operator"),
                now);
        store.cancelWorkflow(recovered.revision(), cancelled, step.invocationId());
        assertEquals(WorkflowStatus.CANCELLED, store.load(initial.id()).orElseThrow().status());
        assertEquals(StepInvocationStatus.CANCELLED, store.loadStep(step.invocationId()).orElseThrow().status());
    }

    @Test
    void inventoriesDefinitionsWithoutDecodingAndArchivesTerminalWorkflows() {
        Instant now = Instant.now();
        WorkflowRecord completed = ready("archive-1", now).next(
                WorkflowStatus.COMPLETED,
                null,
                null,
                null,
                "done",
                null,
                0,
                null,
                List.of(),
                new HistoryEntry(now, "COMPLETED", "done"),
                now);
        store.create(completed);

        assertEquals(0, store.definitionInventory().size(), "terminal definitions are not resumable inventory");
        ArchiveResult result = store.archive(ArchiveRequest.completedBefore(now.plusSeconds(1)));
        assertEquals(1, result.count());
        assertTrue(store.load(completed.id()).isEmpty());
        var archived = store.loadArchived(result.archived().get(0).archiveId()).orElseThrow();
        assertEquals("done", archived.workflow().output());
        assertEquals(WorkflowStatus.COMPLETED, archived.summary().finalStatus());
        assertEquals(1, store.listArchived(10, 0).size());
    }

    @Test
    void definitionInventoryGroupsResumableExactIdentities() {
        Instant now = Instant.now();
        store.create(ready("inventory-1", now));
        store.create(ready("inventory-2", now));
        var inventory = store.definitionInventory();
        assertEquals(1, inventory.size());
        assertEquals(2L, inventory.get(0).workflowCount());
        assertEquals(WorkflowStatus.READY, inventory.get(0).status());
    }

    private WorkflowRecord ready(String id, Instant now) {
        return new WorkflowRecord(
                id,
                0L,
                "definition",
                1,
                "hash",
                WorkflowStatus.READY,
                new byte[] {1, 2, 3},
                null,
                new ResumeValue.Success(null),
                null,
                null,
                0,
                now,
                now,
                now,
                Map.of(),
                List.of(new SignalEnvelope("early", (Serializable) "early-value", now)),
                List.of(new HistoryEntry(now, "STARTED", "definition")));
    }

    @Test
    void databaseGlobalStepConcurrencyAndStuckInspection() {
        store.registerStepConcurrencyLimit("limited", 1);
        assertEquals(1, store.stepConcurrencyLimits().get("limited"));

        Instant old = Instant.now().minus(Duration.ofMinutes(10));
        StepInvocationRecord first = createPendingStep("limit-1", "limited", old);
        StepInvocationRecord second = createPendingStep("limit-2", "limited", old);

        ClaimedStep claimed = store.tryClaimStep(
                        first.invocationId(), "worker-a", Duration.ofMinutes(1))
                .orElseThrow();
        assertFalse(store.tryClaimStep(
                        second.invocationId(), "worker-b", Duration.ofMinutes(1))
                .isPresent(), "database-global limit must block a second replica");

        var report = store.scanStuckWork(Duration.ofMinutes(1), 20);
        assertTrue(report.workflows().stream().anyMatch(item -> item.workflowId().equals("limit-2")));
        assertTrue(report.steps().stream().anyMatch(item -> item.invocationId().equals("limit-2:1")));
        store.releaseStepLease(claimed.lease());
    }

    private StepInvocationRecord createPendingStep(String workflowId, String stepName, Instant now) {
        WorkflowRecord initial = ready(workflowId, now);
        store.create(initial);
        WorkflowLease lease = store.tryAcquireWorkflowLease(
                        workflowId, "setup-" + workflowId, Duration.ofSeconds(10))
                .orElseThrow();
        DurableCommand.Step command = new DurableCommand.Step(
                1L, stepName, "argument", StepOptions.retry(2, Duration.ZERO));
        WorkflowRecord waiting = initial.next(
                WorkflowStatus.WAITING, initial.continuation(), command, null, null, null, 0, now,
                initial.signals(), new HistoryEntry(now, "SUSPENDED", stepName), now);
        StepInvocationRecord step = StepInvocationRecord.pending(
                workflowId + ":1", workflowId, 1L, stepName, "argument", command.options(), now);
        store.suspendWithStep(lease, initial.revision(), waiting, step);
        store.releaseWorkflowLease(lease);
        return step;
    }

    private void createPortableSchema() throws Exception {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table durable_workflow (
                        workflow_id varchar(200) primary key,
                        revision bigint not null,
                        definition_id varchar(200) not null,
                        definition_version integer not null,
                        definition_hash varchar(64) not null,
                        status varchar(32) not null,
                        wait_kind varchar(32),
                        wait_key varchar(300),
                        available_at timestamp,
                        record_blob longvarbinary not null,
                        lease_owner varchar(200),
                        lease_token bigint default 0 not null,
                        lease_expires_at timestamp,
                        created_at timestamp not null,
                        updated_at timestamp not null
                    )
                    """);
            statement.execute("""
                    create table durable_signal (
                        signal_id varchar(36) primary key,
                        workflow_id varchar(200) not null,
                        signal_position integer not null,
                        signal_name varchar(200) not null,
                        payload_blob longvarbinary,
                        received_at timestamp not null,
                        unique (workflow_id, signal_position)
                    )
                    """);
            statement.execute("""
                    create table durable_step_invocation (
                        invocation_id varchar(300) primary key,
                        workflow_id varchar(200) not null,
                        sequence_number bigint not null,
                        step_name varchar(200) not null,
                        status varchar(32) not null,
                        attempt integer not null,
                        maximum_attempts integer not null,
                        available_at timestamp not null,
                        started_at timestamp,
                        deadline timestamp,
                        record_blob longvarbinary not null,
                        lease_owner varchar(200),
                        lease_token bigint default 0 not null,
                        lease_expires_at timestamp,
                        created_at timestamp not null,
                        updated_at timestamp not null,
                        unique (workflow_id, sequence_number)
                    )
                    """);
            statement.execute("""
                    create table durable_step_concurrency_limit (
                        step_name varchar(200) primary key,
                        max_concurrency integer not null,
                        updated_at timestamp not null,
                        check (max_concurrency > 0)
                    )
                    """);
            statement.execute("""
                    create table durable_workflow_archive (
                        archive_id varchar(36) primary key,
                        workflow_id varchar(200) not null,
                        definition_id varchar(200) not null,
                        definition_version integer not null,
                        definition_hash varchar(64) not null,
                        final_status varchar(32) not null,
                        final_revision bigint not null,
                        workflow_blob longvarbinary not null,
                        steps_blob longvarbinary not null,
                        created_at timestamp not null,
                        updated_at timestamp not null,
                        archived_at timestamp not null
                    )
                    """);
        }
    }
}
