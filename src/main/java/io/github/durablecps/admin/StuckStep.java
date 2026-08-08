package io.github.durablecps.admin;

import io.github.durablecps.store.StepInvocationStatus;
import java.time.Instant;

public record StuckStep(
        String invocationId,
        String workflowId,
        String stepName,
        StepInvocationStatus status,
        int attempt,
        Instant availableAt,
        Instant deadline,
        Instant updatedAt,
        String leaseOwner,
        Instant leaseExpiresAt,
        String reason) {
    public StuckStep {
        leaseOwner = leaseOwner == null ? "" : leaseOwner;
        reason = reason == null ? "" : reason;
    }
}
