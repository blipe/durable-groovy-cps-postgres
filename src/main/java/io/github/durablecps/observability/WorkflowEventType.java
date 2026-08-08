package io.github.durablecps.observability;

public enum WorkflowEventType {
    STARTED,
    SUSPENDED,
    RESUMED,
    SIGNAL_RECEIVED,
    SIGNAL_DELIVERED,
    TIMER_COMPLETED,
    COMPLETED,
    FAILED,
    DEAD_LETTERED,
    RECOVERED,
    CANCELLED,
    STATE_CHANGED
}
