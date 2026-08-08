package io.github.durablecps.store;

import io.github.durablecps.admin.ArchiveRequest;
import io.github.durablecps.admin.ArchiveResult;
import io.github.durablecps.admin.ArchivedWorkflow;
import io.github.durablecps.admin.ArchivedWorkflowSummary;
import io.github.durablecps.admin.WorkflowQuery;
import io.github.durablecps.admin.StuckStep;
import io.github.durablecps.admin.StuckWorkflow;
import io.github.durablecps.admin.StuckWorkReport;
import io.github.durablecps.api.ConcurrentWorkflowUpdateException;
import io.github.durablecps.api.DurableCommand;
import io.github.durablecps.api.WorkflowStatus;
import io.github.durablecps.runtime.JavaObjectCodec;
import io.github.durablecps.runtime.ObjectCodec;
import io.github.durablecps.runtime.state.SignalEnvelope;
import io.github.durablecps.runtime.state.WorkflowRecord;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * PostgreSQL-oriented production store with transactional state changes and fenced leases.
 *
 * <p>The store does not own the {@link DataSource}. Call {@link #migrate()} once during application
 * startup, before starting engine pollers.</p>
 */
public final class JdbcWorkflowStore implements CoordinatedWorkflowStore, SearchableWorkflowStore,
        DefinitionInventoryStore, ArchiveCapableWorkflowStore, SchemaManagedWorkflowStore,
        StepConcurrencyStore, StuckWorkStore {
    private final DataSource dataSource;
    private final ObjectCodec codec;
    private final ClassLoader classLoader;

    public JdbcWorkflowStore(DataSource dataSource) {
        this(dataSource, new JavaObjectCodec(), JdbcWorkflowStore.class.getClassLoader());
    }

    public JdbcWorkflowStore(DataSource dataSource, ObjectCodec codec) {
        this(dataSource, codec, JdbcWorkflowStore.class.getClassLoader());
    }

    public JdbcWorkflowStore(DataSource dataSource, ObjectCodec codec, ClassLoader classLoader) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.classLoader = Objects.requireNonNull(classLoader, "classLoader");
    }

    /** Applies all pending checksummed migrations. Kept for source compatibility with earlier releases. */
    public void migrate() {
        migrateSchema();
    }

    @Override
    public SchemaStatus schemaStatus() {
        return new JdbcSchemaMigrator(dataSource).status();
    }

    @Override
    public SchemaStatus migrateSchema() {
        return new JdbcSchemaMigrator(dataSource).migrate();
    }

    @Override
    public WorkflowRecord create(WorkflowRecord initial) {
        Objects.requireNonNull(initial, "initial");
        return inTransaction(connection -> {
            String sql = """
                    insert into durable_workflow (
                        workflow_id, revision, definition_id, definition_version, definition_hash,
                        status, wait_kind, wait_key, available_at, record_blob,
                        created_at, updated_at
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """;
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                bindWorkflow(statement, initial, 1);
                statement.executeUpdate();
            } catch (SQLException e) {
                if (isConstraintViolation(e)) {
                    throw new IllegalStateException("workflow already exists: " + initial.id(), e);
                }
                throw e;
            }
            replaceSignals(connection, initial.id(), initial.signals());
            return initial;
        });
    }

    @Override
    public Optional<WorkflowRecord> load(String workflowId) {
        Objects.requireNonNull(workflowId, "workflowId");
        return withConnection(connection -> load(connection, workflowId, false));
    }

    @Override
    public WorkflowRecord replace(String workflowId, long expectedRevision, WorkflowRecord replacement) {
        Objects.requireNonNull(workflowId, "workflowId");
        Objects.requireNonNull(replacement, "replacement");
        return inTransaction(connection -> {
            int changed = updateWorkflow(connection, workflowId, expectedRevision, replacement, null);
            if (changed != 1) throw concurrent(workflowId, expectedRevision);
            replaceSignals(connection, workflowId, replacement.signals());
            return replacement;
        });
    }

    @Override
    public Collection<WorkflowRecord> list() {
        return withConnection(connection -> {
            List<String> ids = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(
                            "select workflow_id from durable_workflow order by created_at, workflow_id");
                    ResultSet rows = statement.executeQuery()) {
                while (rows.next()) ids.add(rows.getString(1));
            }
            List<WorkflowRecord> records = new ArrayList<>(ids.size());
            for (String id : ids) load(connection, id, false).ifPresent(records::add);
            return List.copyOf(records);
        });
    }

    @Override
    public Instant currentTime() {
        return withConnection(JdbcWorkflowStore::databaseNow);
    }

    @Override
    public Collection<String> findRunnableWorkflowIds(int limit) {
        checkLimit(limit);
        return withConnection(connection -> {
            String sql = """
                    select w.workflow_id
                    from durable_workflow w
                    where
                        w.status = 'READY'
                        or (w.status = 'WAITING' and w.wait_kind = 'SLEEP'
                            and w.available_at <= current_timestamp)
                        or (w.status = 'WAITING' and w.wait_kind = 'SIGNAL'
                            and exists (
                                select 1 from durable_signal s
                                where s.workflow_id = w.workflow_id and s.signal_name = w.wait_key
                            ))
                    order by coalesce(w.available_at, w.updated_at), w.workflow_id
                    limit ?
                    """;
            List<String> ids = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, limit);
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) ids.add(rows.getString(1));
                }
            }
            return List.copyOf(ids);
        });
    }

    @Override
    public Collection<String> findDueStepInvocationIds(int limit) {
        checkLimit(limit);
        return withConnection(connection -> {
            String sql = """
                    select invocation_id
                    from durable_step_invocation
                    where
                        (status in ('PENDING', 'RETRY_WAIT') and available_at <= current_timestamp)
                        or (status = 'RUNNING' and (
                            lease_expires_at is null or lease_expires_at <= current_timestamp
                            or deadline <= current_timestamp
                        ))
                    order by available_at, invocation_id
                    limit ?
                    """;
            List<String> ids = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, limit);
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) ids.add(rows.getString(1));
                }
            }
            return List.copyOf(ids);
        });
    }

    @Override
    public void registerStepConcurrencyLimit(String stepName, int maximumConcurrent) {
        Objects.requireNonNull(stepName, "stepName");
        if (stepName.isBlank()) throw new IllegalArgumentException("stepName must not be blank");
        if (maximumConcurrent < 1) throw new IllegalArgumentException("maximumConcurrent must be >= 1");
        inTransaction(connection -> {
            acquireStepLimitLock(connection, stepName);
            try (PreparedStatement select = connection.prepareStatement(
                    "select max_concurrency from durable_step_concurrency_limit where step_name = ?"
                            + forUpdate(connection))) {
                select.setString(1, stepName);
                try (ResultSet row = select.executeQuery()) {
                    if (row.next()) {
                        int existing = row.getInt(1);
                        if (existing != maximumConcurrent) {
                            throw new IllegalStateException(
                                    "step concurrency mismatch for " + stepName
                                            + ": database=" + existing
                                            + " configured=" + maximumConcurrent);
                        }
                        return null;
                    }
                }
            }
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into durable_step_concurrency_limit
                        (step_name, max_concurrency, updated_at)
                    values (?, ?, current_timestamp)
                    """)) {
                insert.setString(1, stepName);
                insert.setInt(2, maximumConcurrent);
                insert.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public Map<String, Integer> stepConcurrencyLimits() {
        return withConnection(connection -> {
            java.util.LinkedHashMap<String, Integer> result = new java.util.LinkedHashMap<>();
            try (PreparedStatement statement = connection.prepareStatement(
                            "select step_name, max_concurrency from durable_step_concurrency_limit order by step_name");
                    ResultSet rows = statement.executeQuery()) {
                while (rows.next()) result.put(rows.getString(1), rows.getInt(2));
            }
            return Map.copyOf(result);
        });
    }

    @Override
    public StuckWorkReport scanStuckWork(Duration overdueBy, int limit) {
        Objects.requireNonNull(overdueBy, "overdueBy");
        if (overdueBy.isNegative()) throw new IllegalArgumentException("overdueBy must not be negative");
        if (limit < 1 || limit > 10_000) throw new IllegalArgumentException("limit must be 1..10000");
        return withConnection(connection -> {
            Instant now = databaseNow(connection);
            Instant cutoff = now.minus(overdueBy);
            List<StuckWorkflow> workflows = new ArrayList<>();
            String workflowSql = """
                    select workflow_id, status, wait_kind, available_at, updated_at,
                           lease_owner, lease_expires_at
                    from durable_workflow
                    where (status = 'READY' and updated_at <= ?)
                       or (status = 'WAITING' and wait_kind = 'SLEEP' and available_at <= ?)
                    order by coalesce(available_at, updated_at), workflow_id
                    """;
            try (PreparedStatement statement = connection.prepareStatement(workflowSql)) {
                setInstant(statement, 1, cutoff);
                setInstant(statement, 2, cutoff);
                statement.setMaxRows(limit + 1);
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next() && workflows.size() <= limit) {
                        WorkflowStatus status = WorkflowStatus.valueOf(rows.getString(2));
                        String waitKind = rows.getString(3);
                        workflows.add(new StuckWorkflow(
                                rows.getString(1), status, waitKind, instant(rows.getTimestamp(4)),
                                instant(rows.getTimestamp(5)), rows.getString(6),
                                instant(rows.getTimestamp(7)),
                                status == WorkflowStatus.READY
                                        ? "ready workflow has not been driven"
                                        : "durable timer is overdue"));
                    }
                }
            }

            List<StuckStep> steps = new ArrayList<>();
            String stepSql = """
                    select invocation_id, workflow_id, step_name, status, attempt, available_at,
                           deadline, updated_at, lease_owner, lease_expires_at
                    from durable_step_invocation
                    where (status in ('PENDING', 'RETRY_WAIT') and available_at <= ?)
                       or (status = 'RUNNING' and (
                              (deadline is not null and deadline <= ?)
                              or (lease_expires_at is not null and lease_expires_at <= ?)
                           ))
                    order by coalesce(deadline, lease_expires_at, available_at), invocation_id
                    """;
            try (PreparedStatement statement = connection.prepareStatement(stepSql)) {
                setInstant(statement, 1, cutoff);
                setInstant(statement, 2, cutoff);
                setInstant(statement, 3, cutoff);
                statement.setMaxRows(limit + 1);
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next() && steps.size() <= limit) {
                        StepInvocationStatus status = StepInvocationStatus.valueOf(rows.getString(4));
                        Instant deadline = instant(rows.getTimestamp(7));
                        Instant leaseExpiry = instant(rows.getTimestamp(10));
                        String reason = status == StepInvocationStatus.RUNNING
                                ? deadline != null && !deadline.isAfter(cutoff)
                                        ? "step execution deadline is overdue"
                                        : "step lease expired without recovery"
                                : "step has been claimable without execution";
                        steps.add(new StuckStep(
                                rows.getString(1), rows.getString(2), rows.getString(3), status,
                                rows.getInt(5), instant(rows.getTimestamp(6)), deadline,
                                instant(rows.getTimestamp(8)), rows.getString(9), leaseExpiry, reason));
                    }
                }
            }
            boolean truncated = workflows.size() > limit || steps.size() > limit;
            if (workflows.size() > limit) workflows = new ArrayList<>(workflows.subList(0, limit));
            if (steps.size() > limit) steps = new ArrayList<>(steps.subList(0, limit));
            return new StuckWorkReport(now, overdueBy, workflows, steps, truncated);
        });
    }

    @Override
    public Optional<WorkflowLease> tryAcquireWorkflowLease(
            String workflowId, String owner, Duration leaseDuration) {
        validateLease(owner, leaseDuration);
        return inTransaction(connection -> {
            String select = """
                    select lease_owner, lease_token, lease_expires_at, current_timestamp
                    from durable_workflow where workflow_id = ?
                    """ + forUpdate(connection);
            try (PreparedStatement statement = connection.prepareStatement(select)) {
                statement.setString(1, workflowId);
                try (ResultSet row = statement.executeQuery()) {
                    if (!row.next()) return Optional.empty();
                    String currentOwner = row.getString(1);
                    long token = row.getLong(2);
                    Instant expires = instant(row.getTimestamp(3));
                    Instant now = instant(row.getTimestamp(4));
                    if (currentOwner != null && !owner.equals(currentOwner) && expires != null && expires.isAfter(now)) {
                        return Optional.empty();
                    }
                    long newToken = token + 1;
                    Instant newExpiry = safePlus(now, leaseDuration);
                    try (PreparedStatement update = connection.prepareStatement("""
                            update durable_workflow
                            set lease_owner = ?, lease_token = ?, lease_expires_at = ?
                            where workflow_id = ?
                            """)) {
                        update.setString(1, owner);
                        update.setLong(2, newToken);
                        setInstant(update, 3, newExpiry);
                        update.setString(4, workflowId);
                        update.executeUpdate();
                    }
                    return Optional.of(new WorkflowLease(workflowId, owner, newToken, newExpiry));
                }
            }
        });
    }

    @Override
    public boolean renewWorkflowLease(WorkflowLease lease, Duration leaseDuration) {
        validateLease(lease.owner(), leaseDuration);
        return inTransaction(connection -> {
            Instant now = databaseNow(connection);
            Instant expiry = safePlus(now, leaseDuration);
            try (PreparedStatement statement = connection.prepareStatement("""
                    update durable_workflow
                    set lease_expires_at = ?
                    where workflow_id = ? and lease_owner = ? and lease_token = ?
                      and lease_expires_at > current_timestamp
                    """)) {
                setInstant(statement, 1, expiry);
                statement.setString(2, lease.workflowId());
                statement.setString(3, lease.owner());
                statement.setLong(4, lease.token());
                return statement.executeUpdate() == 1;
            }
        });
    }

    @Override
    public void releaseWorkflowLease(WorkflowLease lease) {
        inTransaction(connection -> {
            try (PreparedStatement statement = connection.prepareStatement("""
                    update durable_workflow
                    set lease_owner = null, lease_expires_at = null
                    where workflow_id = ? and lease_owner = ? and lease_token = ?
                    """)) {
                statement.setString(1, lease.workflowId());
                statement.setString(2, lease.owner());
                statement.setLong(3, lease.token());
                statement.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public WorkflowRecord replaceWithLease(
            WorkflowLease lease, long expectedRevision, WorkflowRecord replacement) {
        return inTransaction(connection -> {
            int changed = updateWorkflow(connection, lease.workflowId(), expectedRevision, replacement, lease);
            if (changed != 1) throw leaseOrConcurrent(connection, lease, expectedRevision);
            replaceSignals(connection, lease.workflowId(), replacement.signals());
            return replacement;
        });
    }

    @Override
    public WorkflowRecord suspendWithStep(
            WorkflowLease lease,
            long expectedRevision,
            WorkflowRecord waitingWorkflow,
            StepInvocationRecord pendingStep) {
        return inTransaction(connection -> {
            int changed = updateWorkflow(connection, lease.workflowId(), expectedRevision, waitingWorkflow, lease);
            if (changed != 1) throw leaseOrConcurrent(connection, lease, expectedRevision);
            insertStep(connection, pendingStep);
            replaceSignals(connection, lease.workflowId(), waitingWorkflow.signals());
            return waitingWorkflow;
        });
    }

    @Override
    public Optional<ClaimedStep> tryClaimStep(
            String invocationId, String owner, Duration leaseDuration) {
        validateLease(owner, leaseDuration);
        return inTransaction(connection -> {
            Optional<StepInvocationRecord> found = loadStep(connection, invocationId, true);
            if (found.isEmpty()) return Optional.empty();
            StepInvocationRecord current = found.get();
            if (current.status().terminal()) return Optional.empty();

            Instant now = databaseNow(connection);
            LeaseRow leaseRow = loadStepLease(connection, invocationId);
            boolean normalDue = (current.status() == StepInvocationStatus.PENDING
                            || current.status() == StepInvocationStatus.RETRY_WAIT)
                    && !current.availableAt().isAfter(now);
            boolean abandoned = current.status() == StepInvocationStatus.RUNNING
                    && (leaseRow.expiresAt() == null
                            || !leaseRow.expiresAt().isAfter(now)
                            || (current.deadline() != null && !current.deadline().isAfter(now)));
            if (!normalDue && !abandoned) return Optional.empty();
            if (!hasStepCapacity(connection, current.stepName())) return Optional.empty();

            int attempt = current.attempt() + 1;
            Instant deadline = safePlus(now, current.options().executionTimeout());
            StepInvocationRecord running = current.running(attempt, now, deadline);
            long token = leaseRow.token() + 1;
            Instant leaseExpiry = safePlus(now, leaseDuration);
            try (PreparedStatement update = connection.prepareStatement("""
                    update durable_step_invocation
                    set status = 'RUNNING', attempt = ?, started_at = ?, deadline = ?,
                        record_blob = ?, lease_owner = ?, lease_token = ?, lease_expires_at = ?, updated_at = ?
                    where invocation_id = ?
                    """)) {
                update.setInt(1, attempt);
                setInstant(update, 2, now);
                setInstant(update, 3, deadline);
                update.setBytes(4, codec.encode(running));
                update.setString(5, owner);
                update.setLong(6, token);
                setInstant(update, 7, leaseExpiry);
                setInstant(update, 8, now);
                update.setString(9, invocationId);
                update.executeUpdate();
            }
            return Optional.of(new ClaimedStep(running, new StepLease(invocationId, owner, token, leaseExpiry)));
        });
    }

    @Override
    public boolean renewStepLease(StepLease lease, Duration leaseDuration) {
        validateLease(lease.owner(), leaseDuration);
        return inTransaction(connection -> {
            Instant expiry = safePlus(databaseNow(connection), leaseDuration);
            try (PreparedStatement statement = connection.prepareStatement("""
                    update durable_step_invocation
                    set lease_expires_at = ?
                    where invocation_id = ? and lease_owner = ? and lease_token = ?
                      and status = 'RUNNING' and lease_expires_at > current_timestamp
                      and (deadline is null or deadline > current_timestamp)
                    """)) {
                setInstant(statement, 1, expiry);
                statement.setString(2, lease.invocationId());
                statement.setString(3, lease.owner());
                statement.setLong(4, lease.token());
                return statement.executeUpdate() == 1;
            }
        });
    }

    @Override
    public void releaseStepLease(StepLease lease) {
        inTransaction(connection -> {
            try (PreparedStatement statement = connection.prepareStatement("""
                    update durable_step_invocation
                    set lease_owner = null, lease_expires_at = null
                    where invocation_id = ? and lease_owner = ? and lease_token = ?
                    """)) {
                statement.setString(1, lease.invocationId());
                statement.setString(2, lease.owner());
                statement.setLong(3, lease.token());
                statement.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public WorkflowRecord completeStep(
            StepLease lease,
            long expectedWorkflowRevision,
            WorkflowRecord workflowReplacement,
            StepInvocationRecord stepReplacement) {
        return inTransaction(connection -> {
            LeaseRow currentLease = loadStepLeaseForUpdate(connection, lease.invocationId());
            Instant now = databaseNow(connection);
            if (!lease.owner().equals(currentLease.owner())
                    || lease.token() != currentLease.token()
                    || currentLease.expiresAt() == null
                    || !currentLease.expiresAt().isAfter(now)
                    || (currentLease.deadline() != null && !currentLease.deadline().isAfter(now))) {
                throw new LeaseLostException("step lease lost: " + lease.invocationId());
            }
            int changed = updateWorkflow(
                    connection,
                    workflowReplacement.id(),
                    expectedWorkflowRevision,
                    workflowReplacement,
                    null);
            if (changed != 1) throw concurrent(workflowReplacement.id(), expectedWorkflowRevision);
            updateStep(connection, stepReplacement, lease);
            replaceSignals(connection, workflowReplacement.id(), workflowReplacement.signals());
            return workflowReplacement;
        });
    }

    @Override
    public WorkflowRecord recoverStep(long expectedWorkflowRevision, WorkflowRecord workflowReplacement, StepInvocationRecord stepReplacement) {
        return inTransaction(connection -> {
            int changed = updateWorkflow(connection, workflowReplacement.id(), expectedWorkflowRevision, workflowReplacement, null);
            if (changed != 1) throw concurrent(workflowReplacement.id(), expectedWorkflowRevision);
            updateAdministrativeStep(connection, stepReplacement);
            replaceSignals(connection, workflowReplacement.id(), workflowReplacement.signals());
            return workflowReplacement;
        });
    }

    @Override
    public WorkflowRecord cancelWorkflow(long expectedWorkflowRevision, WorkflowRecord workflowReplacement, String pendingInvocationId) {
        return inTransaction(connection -> {
            int changed = updateWorkflow(connection, workflowReplacement.id(), expectedWorkflowRevision, workflowReplacement, null);
            if (changed != 1) throw concurrent(workflowReplacement.id(), expectedWorkflowRevision);
            if (pendingInvocationId != null) {
                Optional<StepInvocationRecord> step = loadStep(connection, pendingInvocationId, true);
                if (step.isPresent() && !step.get().status().terminal()) updateAdministrativeStep(connection, step.get().cancelled(databaseNow(connection)));
            }
            replaceSignals(connection, workflowReplacement.id(), workflowReplacement.signals());
            return workflowReplacement;
        });
    }

    @Override
    public Optional<StepInvocationRecord> loadStep(String invocationId) { return withConnection(connection -> loadStep(connection, invocationId, false)); }

    @Override
    public WorkflowRecordPage search(WorkflowQuery query) {
        return withConnection(connection -> {
            QueryParts parts = queryParts(query);
            long total;
            try (PreparedStatement count = connection.prepareStatement("select count(*) from durable_workflow" + parts.where())) {
                bindQuery(count, parts.parameters(), 1);
                try (ResultSet row = count.executeQuery()) { row.next(); total = row.getLong(1); }
            }
            List<String> ids = new ArrayList<>();
            String sql = "select workflow_id from durable_workflow" + parts.where() + " order by updated_at desc, workflow_id limit ? offset ?";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int index = bindQuery(statement, parts.parameters(), 1);
                statement.setInt(index++, query.limit()); statement.setInt(index, query.offset());
                try (ResultSet rows = statement.executeQuery()) { while (rows.next()) ids.add(rows.getString(1)); }
            }
            List<WorkflowRecord> records = new ArrayList<>();
            for (String id : ids) load(connection, id, false).ifPresent(records::add);
            return new WorkflowRecordPage(records, total);
        });
    }

    @Override
    public ArchiveResult archive(ArchiveRequest request) {
        Objects.requireNonNull(request, "request");
        return inTransaction(connection -> {
            Instant archivedAt = databaseNow(connection);
            List<WorkflowStatus> statuses = request.statuses().stream().sorted().toList();
            String placeholders = String.join(",", java.util.Collections.nCopies(statuses.size(), "?"));
            String sql = "select workflow_id from durable_workflow where updated_at < ? and status in ("
                    + placeholders
                    + ") order by updated_at, workflow_id limit ?"
                    + forUpdateSkipLocked(connection);
            List<String> ids = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int index = 1;
                setInstant(statement, index++, request.updatedBefore());
                for (WorkflowStatus status : statuses) statement.setString(index++, status.name());
                statement.setInt(index, request.limit());
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) ids.add(rows.getString(1));
                }
            }

            List<ArchivedWorkflowSummary> archived = new ArrayList<>(ids.size());
            for (String id : ids) {
                WorkflowRecord record = load(connection, id, true).orElse(null);
                if (record == null || !request.statuses().contains(record.status())) continue;
                List<StepInvocationRecord> steps = loadSteps(connection, id);
                String archiveId = UUID.randomUUID().toString();
                ArchivedWorkflowSummary summary = new ArchivedWorkflowSummary(
                        archiveId,
                        record.id(),
                        record.definitionId(),
                        record.definitionVersion(),
                        record.definitionHash(),
                        record.status(),
                        record.revision(),
                        record.createdAt(),
                        record.updatedAt(),
                        archivedAt);
                try (PreparedStatement insert = connection.prepareStatement("""
                        insert into durable_workflow_archive (
                            archive_id, workflow_id, definition_id, definition_version, definition_hash,
                            final_status, final_revision, workflow_blob, steps_blob,
                            created_at, updated_at, archived_at
                        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """)) {
                    insert.setString(1, summary.archiveId());
                    insert.setString(2, summary.workflowId());
                    insert.setString(3, summary.definitionId());
                    insert.setInt(4, summary.definitionVersion());
                    insert.setString(5, summary.definitionHash());
                    insert.setString(6, summary.finalStatus().name());
                    insert.setLong(7, summary.finalRevision());
                    insert.setBytes(8, codec.encode(record));
                    insert.setBytes(9, codec.encode(new ArrayList<>(steps)));
                    setInstant(insert, 10, summary.createdAt());
                    setInstant(insert, 11, summary.updatedAt());
                    setInstant(insert, 12, summary.archivedAt());
                    insert.executeUpdate();
                }
                deleteByWorkflowId(connection, "durable_signal", id);
                deleteByWorkflowId(connection, "durable_step_invocation", id);
                try (PreparedStatement delete = connection.prepareStatement(
                        "delete from durable_workflow where workflow_id = ?")) {
                    delete.setString(1, id);
                    if (delete.executeUpdate() != 1) throw new SQLException("workflow disappeared during archival: " + id);
                }
                archived.add(summary);
            }
            return new ArchiveResult(archivedAt, archived);
        });
    }

    @Override
    public Optional<ArchivedWorkflow> loadArchived(String archiveId) {
        Objects.requireNonNull(archiveId, "archiveId");
        return withConnection(connection -> {
            try (PreparedStatement statement = connection.prepareStatement("""
                    select workflow_id, definition_id, definition_version, definition_hash,
                           final_status, final_revision, workflow_blob, steps_blob,
                           created_at, updated_at, archived_at
                    from durable_workflow_archive where archive_id = ?
                    """)) {
                statement.setString(1, archiveId);
                try (ResultSet row = statement.executeQuery()) {
                    if (!row.next()) return Optional.empty();
                    ArchivedWorkflowSummary summary = new ArchivedWorkflowSummary(
                            archiveId,
                            row.getString(1),
                            row.getString(2),
                            row.getInt(3),
                            row.getString(4),
                            WorkflowStatus.valueOf(row.getString(5)),
                            row.getLong(6),
                            instant(row.getTimestamp(9)),
                            instant(row.getTimestamp(10)),
                            instant(row.getTimestamp(11)));
                    WorkflowRecord workflow = codec.decode(row.getBytes(7), WorkflowRecord.class, classLoader);
                    ArrayList<?> rawSteps = codec.decode(row.getBytes(8), ArrayList.class, classLoader);
                    List<StepInvocationRecord> steps = rawSteps.stream()
                            .map(StepInvocationRecord.class::cast)
                            .toList();
                    return Optional.of(new ArchivedWorkflow(summary, workflow, steps));
                }
            }
        });
    }

    @Override
    public List<ArchivedWorkflowSummary> listArchived(int limit, int offset) {
        if (limit < 1 || limit > 10_000) throw new IllegalArgumentException("limit must be 1..10000");
        if (offset < 0) throw new IllegalArgumentException("offset must be >= 0");
        return withConnection(connection -> {
            List<ArchivedWorkflowSummary> result = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement("""
                    select archive_id, workflow_id, definition_id, definition_version, definition_hash,
                           final_status, final_revision, created_at, updated_at, archived_at
                    from durable_workflow_archive
                    order by archived_at desc, archive_id
                    limit ? offset ?
                    """)) {
                statement.setInt(1, limit);
                statement.setInt(2, offset);
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) result.add(archivedSummary(rows.getString(1), rows, 2));
                }
            }
            return List.copyOf(result);
        });
    }

    private static void deleteByWorkflowId(Connection connection, String table, String workflowId)
            throws SQLException {
        if (!table.equals("durable_signal") && !table.equals("durable_step_invocation")) {
            throw new IllegalArgumentException("unsupported archive table: " + table);
        }
        try (PreparedStatement delete = connection.prepareStatement(
                "delete from " + table + " where workflow_id = ?")) {
            delete.setString(1, workflowId);
            delete.executeUpdate();
        }
    }

    private List<StepInvocationRecord> loadSteps(Connection connection, String workflowId) throws SQLException {
        List<StepInvocationRecord> result = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement("""
                select record_blob from durable_step_invocation
                where workflow_id = ? order by sequence_number
                """)) {
            statement.setString(1, workflowId);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    result.add(codec.decode(rows.getBytes(1), StepInvocationRecord.class, classLoader));
                }
            }
        }
        return List.copyOf(result);
    }

    private static ArchivedWorkflowSummary archivedSummary(String archiveId, ResultSet row, int offset)
            throws SQLException {
        return new ArchivedWorkflowSummary(
                archiveId,
                row.getString(offset),
                row.getString(offset + 1),
                row.getInt(offset + 2),
                row.getString(offset + 3),
                WorkflowStatus.valueOf(row.getString(offset + 4)),
                row.getLong(offset + 5),
                instant(row.getTimestamp(offset + 6)),
                instant(row.getTimestamp(offset + 7)),
                instant(row.getTimestamp(offset + 8)));
    }

    @Override
    public List<StoredDefinitionReference> definitionInventory() {
        return withConnection(connection -> {
            List<StoredDefinitionReference> result = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement("""
                    select definition_id, definition_version, definition_hash, status, count(*)
                    from durable_workflow
                    where status in ('READY', 'WAITING', 'DEAD_LETTERED')
                    group by definition_id, definition_version, definition_hash, status
                    order by definition_id, definition_version, status
                    """);
                    ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    result.add(new StoredDefinitionReference(
                            rows.getString(1),
                            rows.getInt(2),
                            rows.getString(3),
                            WorkflowStatus.valueOf(rows.getString(4)),
                            rows.getLong(5)));
                }
            }
            return List.copyOf(result);
        });
    }

    @Override
    public Map<WorkflowStatus, Long> countByStatus() {
        return withConnection(connection -> {
            EnumMap<WorkflowStatus, Long> counts = new EnumMap<>(WorkflowStatus.class);
            for (WorkflowStatus status : WorkflowStatus.values()) counts.put(status, 0L);
            try (PreparedStatement statement = connection.prepareStatement(
                            "select status, count(*) from durable_workflow group by status");
                    ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    counts.put(WorkflowStatus.valueOf(rows.getString(1)), rows.getLong(2));
                }
            }
            return Map.copyOf(counts);
        });
    }

    @Override
    public void checkHealth() {
        withConnection(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                            "select count(*) from durable_workflow");
                    ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) throw new IllegalStateException("database health query failed");
            }
            return null;
        });
    }

    private Optional<WorkflowRecord> load(Connection connection, String workflowId, boolean forUpdate)
            throws SQLException {
        String sql = "select record_blob from durable_workflow where workflow_id = ?"
                + (forUpdate ? forUpdate(connection) : "");
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, workflowId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) return Optional.empty();
                WorkflowRecord stored = codec.decode(row.getBytes(1), WorkflowRecord.class, classLoader);
                return Optional.of(withSignals(stored, loadSignals(connection, workflowId)));
            }
        }
    }

    private Optional<StepInvocationRecord> loadStep(
            Connection connection, String invocationId, boolean forUpdate) throws SQLException {
        String sql = "select record_blob from durable_step_invocation where invocation_id = ?"
                + (forUpdate ? forUpdate(connection) : "");
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, invocationId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) return Optional.empty();
                return Optional.of(codec.decode(row.getBytes(1), StepInvocationRecord.class, classLoader));
            }
        }
    }

    private int updateWorkflow(
            Connection connection,
            String workflowId,
            long expectedRevision,
            WorkflowRecord replacement,
            WorkflowLease lease)
            throws SQLException {
        String leasePredicate = lease == null
                ? ""
                : " and lease_owner = ? and lease_token = ? and lease_expires_at > current_timestamp";
        String sql = """
                update durable_workflow
                set revision = ?, definition_id = ?, definition_version = ?, definition_hash = ?,
                    status = ?, wait_kind = ?, wait_key = ?, available_at = ?, record_blob = ?, updated_at = ?
                where workflow_id = ? and revision = ?
                """ + leasePredicate;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            statement.setLong(index++, replacement.revision());
            statement.setString(index++, replacement.definitionId());
            statement.setInt(index++, replacement.definitionVersion());
            statement.setString(index++, replacement.definitionHash());
            statement.setString(index++, replacement.status().name());
            statement.setString(index++, waitKind(replacement.pendingCommand()));
            statement.setString(index++, waitKey(replacement.pendingCommand()));
            setInstant(statement, index++, replacement.availableAt());
            statement.setBytes(index++, codec.encode(stripSignals(replacement)));
            setInstant(statement, index++, replacement.updatedAt());
            statement.setString(index++, workflowId);
            statement.setLong(index++, expectedRevision);
            if (lease != null) {
                statement.setString(index++, lease.owner());
                statement.setLong(index, lease.token());
            }
            return statement.executeUpdate();
        }
    }

    private void bindWorkflow(PreparedStatement statement, WorkflowRecord record, int start) throws SQLException {
        int index = start;
        statement.setString(index++, record.id());
        statement.setLong(index++, record.revision());
        statement.setString(index++, record.definitionId());
        statement.setInt(index++, record.definitionVersion());
        statement.setString(index++, record.definitionHash());
        statement.setString(index++, record.status().name());
        statement.setString(index++, waitKind(record.pendingCommand()));
        statement.setString(index++, waitKey(record.pendingCommand()));
        setInstant(statement, index++, record.availableAt());
        statement.setBytes(index++, codec.encode(stripSignals(record)));
        setInstant(statement, index++, record.createdAt());
        setInstant(statement, index, record.updatedAt());
    }

    private void insertStep(Connection connection, StepInvocationRecord step) throws SQLException {
        String sql = """
                insert into durable_step_invocation (
                    invocation_id, workflow_id, sequence_number, step_name, status, attempt,
                    maximum_attempts, available_at, started_at, deadline, record_blob, created_at, updated_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindStep(statement, step);
            statement.executeUpdate();
        } catch (SQLException error) {
            if (!isConstraintViolation(error)) throw error;
            StepInvocationRecord existing = loadStep(connection, step.invocationId(), true)
                    .orElseThrow(() -> new IllegalStateException("conflicting step disappeared: " + step.invocationId()));
            if (!existing.workflowId().equals(step.workflowId())
                    || existing.sequence() != step.sequence()
                    || !existing.stepName().equals(step.stepName())) {
                throw new IllegalStateException("invocation id collision: " + step.invocationId(), error);
            }
        }
    }

    private void updateStep(Connection connection, StepInvocationRecord step, StepLease lease) throws SQLException {
        String sql = """
                update durable_step_invocation
                set status = ?, attempt = ?, available_at = ?, started_at = ?, deadline = ?,
                    record_blob = ?, lease_owner = null, lease_expires_at = null, updated_at = ?
                where invocation_id = ? and lease_owner = ? and lease_token = ? and status = 'RUNNING'
                  and lease_expires_at > current_timestamp
                  and (deadline is null or deadline > current_timestamp)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, step.status().name());
            statement.setInt(2, step.attempt());
            setInstant(statement, 3, step.availableAt());
            setInstant(statement, 4, step.startedAt());
            setInstant(statement, 5, step.deadline());
            statement.setBytes(6, codec.encode(step));
            setInstant(statement, 7, step.updatedAt());
            statement.setString(8, step.invocationId());
            statement.setString(9, lease.owner());
            statement.setLong(10, lease.token());
            if (statement.executeUpdate() != 1) {
                throw new LeaseLostException("step lease lost while completing: " + step.invocationId());
            }
        }
    }

    private void updateAdministrativeStep(Connection connection, StepInvocationRecord step) throws SQLException {
        String sql = """
                update durable_step_invocation
                set status = ?, attempt = ?, available_at = ?, started_at = ?, deadline = ?,
                    record_blob = ?, lease_owner = null, lease_expires_at = null, updated_at = ?
                where invocation_id = ? and workflow_id = ? and sequence_number = ?
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, step.status().name());
            statement.setInt(2, step.attempt());
            setInstant(statement, 3, step.availableAt());
            setInstant(statement, 4, step.startedAt());
            setInstant(statement, 5, step.deadline());
            statement.setBytes(6, codec.encode(step));
            setInstant(statement, 7, step.updatedAt());
            statement.setString(8, step.invocationId());
            statement.setString(9, step.workflowId());
            statement.setLong(10, step.sequence());
            if (statement.executeUpdate() != 1) {
                throw new IllegalArgumentException(
                        "step does not exist for recovery: " + step.invocationId());
            }
        }
    }

    private void bindStep(PreparedStatement statement, StepInvocationRecord step) throws SQLException {
        statement.setString(1, step.invocationId());
        statement.setString(2, step.workflowId());
        statement.setLong(3, step.sequence());
        statement.setString(4, step.stepName());
        statement.setString(5, step.status().name());
        statement.setInt(6, step.attempt());
        statement.setInt(7, step.options().maxAttempts());
        setInstant(statement, 8, step.availableAt());
        setInstant(statement, 9, step.startedAt());
        setInstant(statement, 10, step.deadline());
        statement.setBytes(11, codec.encode(step));
        setInstant(statement, 12, step.createdAt());
        setInstant(statement, 13, step.updatedAt());
    }

    private void replaceSignals(Connection connection, String workflowId, List<SignalEnvelope> signals)
            throws SQLException {
        try (PreparedStatement delete = connection.prepareStatement(
                "delete from durable_signal where workflow_id = ?")) {
            delete.setString(1, workflowId);
            delete.executeUpdate();
        }
        if (signals.isEmpty()) return;
        String sql = """
                insert into durable_signal (
                    signal_id, workflow_id, signal_position, signal_name, payload_blob, received_at
                ) values (?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement insert = connection.prepareStatement(sql)) {
            for (int i = 0; i < signals.size(); i++) {
                SignalEnvelope signal = signals.get(i);
                insert.setString(1, UUID.randomUUID().toString());
                insert.setString(2, workflowId);
                insert.setInt(3, i);
                insert.setString(4, signal.name());
                if (signal.payload() == null) insert.setBytes(5, null);
                else insert.setBytes(5, codec.encode(signal.payload()));
                setInstant(insert, 6, signal.receivedAt());
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    private List<SignalEnvelope> loadSignals(Connection connection, String workflowId) throws SQLException {
        String sql = """
                select signal_name, payload_blob, received_at
                from durable_signal where workflow_id = ? order by signal_position
                """;
        List<SignalEnvelope> signals = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, workflowId);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    byte[] payload = rows.getBytes(2);
                    Serializable value = payload == null
                            ? null
                            : codec.decode(payload, Serializable.class, classLoader);
                    signals.add(new SignalEnvelope(rows.getString(1), value, instant(rows.getTimestamp(3))));
                }
            }
        }
        return List.copyOf(signals);
    }

    private boolean hasStepCapacity(Connection connection, String stepName) throws SQLException {
        Integer maximum = null;
        try (PreparedStatement statement = connection.prepareStatement(
                "select max_concurrency from durable_step_concurrency_limit where step_name = ?"
                        + forUpdate(connection))) {
            statement.setString(1, stepName);
            try (ResultSet row = statement.executeQuery()) {
                if (row.next()) maximum = row.getInt(1);
            }
        }
        if (maximum == null) return true;
        try (PreparedStatement statement = connection.prepareStatement("""
                select count(*)
                from durable_step_invocation
                where step_name = ? and status = 'RUNNING'
                  and lease_expires_at > current_timestamp
                  and (deadline is null or deadline > current_timestamp)
                """)) {
            statement.setString(1, stepName);
            try (ResultSet row = statement.executeQuery()) {
                row.next();
                return row.getLong(1) < maximum;
            }
        }
    }

    private static void acquireStepLimitLock(Connection connection, String stepName) throws SQLException {
        String product = connection.getMetaData().getDatabaseProductName();
        if (product != null && product.toLowerCase(java.util.Locale.ROOT).contains("postgresql")) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "select pg_advisory_xact_lock(hashtextextended(?, 0))")) {
                statement.setString(1, "durable-step-limit:" + stepName);
                statement.execute();
            }
        }
    }

    private LeaseRow loadStepLease(Connection connection, String invocationId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select lease_owner, lease_token, lease_expires_at, deadline
                from durable_step_invocation where invocation_id = ?
                """)) {
            statement.setString(1, invocationId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) throw new IllegalArgumentException("step does not exist: " + invocationId);
                return new LeaseRow(
                        row.getString(1), row.getLong(2), instant(row.getTimestamp(3)), instant(row.getTimestamp(4)));
            }
        }
    }

    private LeaseRow loadStepLeaseForUpdate(Connection connection, String invocationId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select lease_owner, lease_token, lease_expires_at, deadline
                from durable_step_invocation where invocation_id = ?
                """ + forUpdate(connection))) {
            statement.setString(1, invocationId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) throw new IllegalArgumentException("step does not exist: " + invocationId);
                return new LeaseRow(
                        row.getString(1), row.getLong(2), instant(row.getTimestamp(3)), instant(row.getTimestamp(4)));
            }
        }
    }

    private RuntimeException leaseOrConcurrent(
            Connection connection, WorkflowLease lease, long expectedRevision) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select revision, lease_owner, lease_token, lease_expires_at, current_timestamp
                from durable_workflow where workflow_id = ?
                """)) {
            statement.setString(1, lease.workflowId());
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) return new IllegalArgumentException("workflow does not exist: " + lease.workflowId());
                long revision = row.getLong(1);
                String owner = row.getString(2);
                long token = row.getLong(3);
                Instant expiry = instant(row.getTimestamp(4));
                Instant now = instant(row.getTimestamp(5));
                if (!lease.owner().equals(owner) || lease.token() != token || expiry == null || !expiry.isAfter(now)) {
                    return new LeaseLostException("workflow lease lost: " + lease.workflowId());
                }
                return new ConcurrentWorkflowUpdateException(
                        lease.workflowId(), expectedRevision, revision);
            }
        }
    }

    private static WorkflowRecord stripSignals(WorkflowRecord record) {
        return new WorkflowRecord(
                record.id(),
                record.revision(),
                record.definitionId(),
                record.definitionVersion(),
                record.definitionHash(),
                record.status(),
                record.continuation(),
                record.pendingCommand(),
                record.resumeValue(),
                record.output(),
                record.failure(),
                record.attempt(),
                record.availableAt(),
                record.createdAt(),
                record.updatedAt(),
                record.telemetryContext(),
                List.of(),
                record.history());
    }

    private static WorkflowRecord withSignals(WorkflowRecord record, List<SignalEnvelope> signals) {
        return new WorkflowRecord(
                record.id(),
                record.revision(),
                record.definitionId(),
                record.definitionVersion(),
                record.definitionHash(),
                record.status(),
                record.continuation(),
                record.pendingCommand(),
                record.resumeValue(),
                record.output(),
                record.failure(),
                record.attempt(),
                record.availableAt(),
                record.createdAt(),
                record.updatedAt(),
                record.telemetryContext(),
                signals,
                record.history());
    }

    private static String waitKind(DurableCommand command) {
        if (command == null) return null;
        if (command instanceof DurableCommand.Step) return "STEP";
        if (command instanceof DurableCommand.Sleep) return "SLEEP";
        if (command instanceof DurableCommand.AwaitSignal) return "SIGNAL";
        throw new IllegalArgumentException("unsupported command: " + command.getClass());
    }

    private static String waitKey(DurableCommand command) {
        if (command instanceof DurableCommand.Step step) return step.name();
        if (command instanceof DurableCommand.AwaitSignal signal) return signal.name();
        return null;
    }

    private static QueryParts queryParts(WorkflowQuery query) {
        List<String> conditions = new ArrayList<>();
        List<Object> parameters = new ArrayList<>();
        if (!query.statuses().isEmpty()) {
            List<WorkflowStatus> statuses = query.statuses().stream().sorted().toList();
            conditions.add("status in (" + String.join(
                    ",", java.util.Collections.nCopies(statuses.size(), "?")) + ")");
            statuses.forEach(status -> parameters.add(status.name()));
        }
        if (query.definitionId() != null) {
            conditions.add("definition_id = ?");
            parameters.add(query.definitionId());
        }
        addTimeCondition(conditions, parameters, "created_at >= ?", query.createdFrom());
        addTimeCondition(conditions, parameters, "created_at <= ?", query.createdTo());
        addTimeCondition(conditions, parameters, "updated_at >= ?", query.updatedFrom());
        addTimeCondition(conditions, parameters, "updated_at <= ?", query.updatedTo());
        return new QueryParts(
                conditions.isEmpty() ? "" : " where " + String.join(" and ", conditions),
                parameters);
    }

    private static void addTimeCondition(
            List<String> conditions, List<Object> parameters, String condition, Instant value) {
        if (value != null) {
            conditions.add(condition);
            parameters.add(value);
        }
    }

    private static int bindQuery(
            PreparedStatement statement, List<Object> parameters, int index) throws SQLException {
        for (Object value : parameters) {
            if (value instanceof Instant instant) setInstant(statement, index++, instant);
            else statement.setObject(index++, value);
        }
        return index;
    }

    private static void validateLease(String owner, Duration duration) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(duration, "duration");
        if (owner.isBlank()) throw new IllegalArgumentException("lease owner must not be blank");
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("lease duration must be positive");
        }
    }

    private static void checkLimit(int limit) {
        if (limit < 1) throw new IllegalArgumentException("limit must be >= 1");
    }

    private static ConcurrentWorkflowUpdateException concurrent(String id, long expectedRevision) {
        return new ConcurrentWorkflowUpdateException(id, expectedRevision, -1L);
    }

    private static boolean isConstraintViolation(SQLException error) {
        return error.getSQLState() != null && error.getSQLState().startsWith("23");
    }


    private static String forUpdate(Connection connection) throws SQLException {
        String product = connection.getMetaData().getDatabaseProductName();
        return product != null && product.toLowerCase(java.util.Locale.ROOT).contains("hsql")
                ? ""
                : " for update";
    }

    private static String forUpdateSkipLocked(Connection connection) throws SQLException {
        String product = connection.getMetaData().getDatabaseProductName();
        return product != null && product.toLowerCase(java.util.Locale.ROOT).contains("hsql")
                ? ""
                : " for update skip locked";
    }

    private static Instant databaseNow(Connection connection) throws SQLException {
        String product = connection.getMetaData().getDatabaseProductName();
        if (product != null && product.toLowerCase(java.util.Locale.ROOT).contains("hsql")) {
            return Instant.now(); // Test-dialect fallback; PostgreSQL remains database-time authoritative.
        }
        try (PreparedStatement statement = connection.prepareStatement("select current_timestamp");
                ResultSet row = statement.executeQuery()) {
            row.next();
            return instant(row.getTimestamp(1));
        }
    }

    private static void setInstant(PreparedStatement statement, int index, Instant value) throws SQLException {
        if (value == null) statement.setTimestamp(index, null);
        else statement.setTimestamp(index, Timestamp.from(value));
    }

    private static Instant instant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static Instant safePlus(Instant instant, Duration duration) {
        try {
            return instant.plus(duration);
        } catch (ArithmeticException e) {
            return Instant.MAX;
        }
    }

    private static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        for (String part : sql.split(";")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) statements.add(trimmed);
        }
        return statements;
    }

    private <T> T withConnection(SqlFunction<Connection, T> operation) {
        try (Connection connection = dataSource.getConnection()) {
            return operation.apply(connection);
        } catch (SQLException e) {
            throw new IllegalStateException("JDBC workflow store failure", e);
        }
    }

    private <T> T inTransaction(SqlFunction<Connection, T> operation) {
        try (Connection connection = dataSource.getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                T result = operation.apply(connection);
                connection.commit();
                return result;
            } catch (Throwable failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                if (failure instanceof RuntimeException runtime) throw runtime;
                if (failure instanceof Error error) throw error;
                throw (SQLException) failure;
            } finally {
                try {
                    connection.setAutoCommit(originalAutoCommit);
                } catch (SQLException ignored) {
                    // Connection is closing.
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("JDBC workflow transaction failed", e);
        }
    }

    @FunctionalInterface
    private interface SqlFunction<T, R> {
        R apply(T value) throws SQLException;
    }

    private record QueryParts(String where, List<Object> parameters) {}
    private record LeaseRow(String owner, long token, Instant expiresAt, Instant deadline) {}
}
