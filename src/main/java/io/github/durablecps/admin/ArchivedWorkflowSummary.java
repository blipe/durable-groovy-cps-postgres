package io.github.durablecps.admin;

import io.github.durablecps.api.WorkflowStatus;
import java.time.Instant;

public record ArchivedWorkflowSummary(
        String archiveId,
        String workflowId,
        String definitionId,
        int definitionVersion,
        String definitionHash,
        WorkflowStatus finalStatus,
        long finalRevision,
        Instant createdAt,
        Instant updatedAt,
        Instant archivedAt) {}
