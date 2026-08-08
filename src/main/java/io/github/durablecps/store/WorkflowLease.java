package io.github.durablecps.store;

import java.time.Instant;
import java.util.Objects;

/** Fenced ownership of one workflow. The token increases every time ownership is reacquired. */
public record WorkflowLease(String workflowId, String owner, long token, Instant expiresAt) {
    public WorkflowLease {
        Objects.requireNonNull(workflowId, "workflowId");
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(expiresAt, "expiresAt");
        if (token < 1) throw new IllegalArgumentException("token must be >= 1");
    }
}
