package io.github.durablecps.admin;

import io.github.durablecps.api.WorkflowStatus;

public record DefinitionCompatibilityIssue(
        String definitionId,
        int definitionVersion,
        String definitionHash,
        WorkflowStatus status,
        long workflowCount,
        String reason) {
    public DefinitionCompatibilityIssue {
        if (workflowCount < 1) throw new IllegalArgumentException("workflowCount must be >= 1");
        reason = reason == null ? "" : reason;
    }
}
