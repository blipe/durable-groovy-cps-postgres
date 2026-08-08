package io.github.durablecps.runtime;

/** Deterministic crash-injection boundaries used by durability tests. */
public enum Failpoint {
    AFTER_WORKFLOW_LEASE_ACQUIRED,
    BEFORE_CONTINUATION_RESUME,
    AFTER_CONTINUATION_RESUME,
    AFTER_CONTINUATION_SERIALIZED,
    AFTER_WORKFLOW_SUSPENDED,
    AFTER_STEP_CLAIMED,
    BEFORE_STEP_HANDLER,
    AFTER_STEP_HANDLER,
    AFTER_STEP_RESULT_STORED
}
