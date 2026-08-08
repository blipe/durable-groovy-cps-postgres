package io.github.durablecps.api;

public final class ConcurrentWorkflowUpdateException extends RuntimeException {
    @java.io.Serial private static final long serialVersionUID = 1L;
    public ConcurrentWorkflowUpdateException(String workflowId, long expectedRevision, long actualRevision) {
        super("workflow '" + workflowId + "' revision changed: expected " + expectedRevision + ", actual " + actualRevision);
    }
}
