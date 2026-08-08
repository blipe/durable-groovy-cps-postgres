package io.github.durablecps.api;

public enum WorkflowStatus {
    READY,
    WAITING,
    COMPLETED,
    FAILED,
    DEAD_LETTERED,
    CANCELLED;

    public boolean terminal() {
        return this == COMPLETED || this == FAILED || this == DEAD_LETTERED || this == CANCELLED;
    }

    public boolean recoverable() {
        return this == DEAD_LETTERED;
    }
}
