package io.github.durablecps.admin;

import io.github.durablecps.api.WorkflowStatus;
import java.time.Duration;
import java.util.Set;

/** Periodic bounded archival policy. Dead-lettered work is excluded unless explicitly requested. */
public record RetentionPolicy(
        Duration terminalAge,
        Duration interval,
        int batchSize,
        Set<WorkflowStatus> statuses) {
    public RetentionPolicy {
        if (terminalAge == null || terminalAge.isNegative()) {
            throw new IllegalArgumentException("terminalAge must not be negative");
        }
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        if (batchSize < 1 || batchSize > 10_000) {
            throw new IllegalArgumentException("batchSize must be 1..10000");
        }
        statuses = Set.copyOf(statuses == null ? Set.of() : statuses);
        if (statuses.isEmpty()) throw new IllegalArgumentException("at least one status is required");
        for (WorkflowStatus status : statuses) {
            if (!status.terminal()) {
                throw new IllegalArgumentException("only terminal statuses may be retained: " + status);
            }
        }
    }

    public static RetentionPolicy standard(Duration terminalAge) {
        return new RetentionPolicy(
                terminalAge,
                Duration.ofHours(1),
                1_000,
                Set.of(WorkflowStatus.COMPLETED, WorkflowStatus.FAILED, WorkflowStatus.CANCELLED));
    }
}
