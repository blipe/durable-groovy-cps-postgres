package io.github.durablecps.store;

import io.github.durablecps.runtime.state.WorkflowRecord;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;

/**
 * Production store contract used for active/active engines.
 *
 * <p>Workflow and step mutations are fenced by monotonically increasing lease tokens. Implementations
 * must make {@link #suspendWithStep} and {@link #completeStep} single database transactions.</p>
 */
public interface CoordinatedWorkflowStore extends WorkflowStore {
    /** Database-authoritative time used for durable scheduling decisions. */
    Instant currentTime();

    Collection<String> findRunnableWorkflowIds(int limit);

    Collection<String> findDueStepInvocationIds(int limit);

    Optional<WorkflowLease> tryAcquireWorkflowLease(String workflowId, String owner, Duration leaseDuration);

    boolean renewWorkflowLease(WorkflowLease lease, Duration leaseDuration);

    void releaseWorkflowLease(WorkflowLease lease);

    WorkflowRecord replaceWithLease(
            WorkflowLease lease, long expectedRevision, WorkflowRecord replacement);

    WorkflowRecord suspendWithStep(
            WorkflowLease lease,
            long expectedRevision,
            WorkflowRecord waitingWorkflow,
            StepInvocationRecord pendingStep);

    Optional<ClaimedStep> tryClaimStep(String invocationId, String owner, Duration leaseDuration);

    boolean renewStepLease(StepLease lease, Duration leaseDuration);

    void releaseStepLease(StepLease lease);

    /**
     * Atomically stores the step state and matching workflow transition. A retry keeps the workflow
     * WAITING; success or exhausted failure moves it READY with a resume value.
     */
    WorkflowRecord completeStep(
            StepLease lease,
            long expectedWorkflowRevision,
            WorkflowRecord workflowReplacement,
            StepInvocationRecord stepReplacement);

    WorkflowRecord recoverStep(long expectedWorkflowRevision, WorkflowRecord workflowReplacement, StepInvocationRecord stepReplacement);
    WorkflowRecord cancelWorkflow(long expectedWorkflowRevision, WorkflowRecord workflowReplacement, String pendingInvocationId);
    Optional<StepInvocationRecord> loadStep(String invocationId);
}
