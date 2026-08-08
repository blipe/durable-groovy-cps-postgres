package io.github.durablecps.store;

import io.github.durablecps.api.WorkflowStatus;

/** Count of persisted workflows sharing an exact definition identity and status. */
public record StoredDefinitionReference(
        String definitionId,
        int definitionVersion,
        String definitionHash,
        WorkflowStatus status,
        long workflowCount) {
    public StoredDefinitionReference {
        if (workflowCount < 1) throw new IllegalArgumentException("workflowCount must be >= 1");
    }
}
