package io.github.durablecps.admin;

/** Runtime admission state for one embedded engine instance. */
public enum EngineLifecycleState {
    /** Polling is enabled and new workflows may be started. */
    RUNNING,
    /** New workflows and polling are stopped while already active work is allowed to finish. */
    QUIESCING,
    /** Executors, definitions, and the workflow store have been closed. */
    CLOSED
}
