package io.github.durablecps.runtime;

@FunctionalInterface
public interface FailureInjector {
    FailureInjector NONE = (point, workflowId, operationId) -> {};

    void hit(Failpoint point, String workflowId, String operationId);
}
