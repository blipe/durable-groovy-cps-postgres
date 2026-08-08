package io.github.durablecps.observability;

public enum StepEventType {
    DISPATCHED,
    COMPLETED,
    RETRY_SCHEDULED,
    FAILED,
    RECOVERED,
    CANCELLED
}
