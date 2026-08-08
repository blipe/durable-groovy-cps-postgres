package io.github.durablecps.admin;

import io.github.durablecps.api.WorkflowView;
import io.github.durablecps.store.SchemaStatus;
import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/** Explicit operational controls; applications decide how to expose these over HTTP or a CLI. */
public interface WorkflowAdministration {
    Optional<WorkflowView> get(String workflowId);

    WorkflowPage search(WorkflowQuery query);

    WorkflowDiagnostic diagnose(String workflowId);

    EngineLifecycleState lifecycle();

    void quiesce();

    void resume();

    Optional<SchemaStatus> schemaStatus();

    void cancel(String workflowId, String reason);

    void retry(String workflowId, String reason);

    void resumeWithValue(String workflowId, Serializable value, String reason);

    void resumeWithFailure(String workflowId, Throwable failure, String reason);

    void deadLetter(String workflowId, String reason);

    DefinitionVerificationReport verifyDefinitions();

    ArchiveResult archive(ArchiveRequest request);

    Optional<ArchivedWorkflow> archived(String archiveId);

    java.util.List<ArchivedWorkflowSummary> archived(int limit, int offset);

    StuckWorkReport stuckWork(Duration overdueBy, int limit);

    Map<String, Integer> stepConcurrencyLimits();

    EngineHealth health();
}
