package io.github.durablecps.admin;

import io.github.durablecps.api.WorkflowStatus;
import java.time.Instant;

public record StuckWorkflow(
        String workflowId,
        WorkflowStatus status,
        String waitKind,
        Instant dueAt,
        Instant updatedAt,
        String leaseOwner,
        Instant leaseExpiresAt,
        String reason) {
    public StuckWorkflow {
        waitKind = waitKind == null ? "" : waitKind;
        leaseOwner = leaseOwner == null ? "" : leaseOwner;
        reason = reason == null ? "" : reason;
    }
}
