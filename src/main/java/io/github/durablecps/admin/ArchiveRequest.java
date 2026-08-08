package io.github.durablecps.admin;

import io.github.durablecps.api.WorkflowStatus;
import java.time.Instant;
import java.util.Set;

/** Bounded archival request. Dead-lettered workflows require explicit inclusion. */
public record ArchiveRequest(Set<WorkflowStatus> statuses, Instant updatedBefore, int limit) {
    public ArchiveRequest {
        statuses = Set.copyOf(statuses == null ? Set.of() : statuses);
        if (statuses.isEmpty()) throw new IllegalArgumentException("at least one status is required");
        for (WorkflowStatus status : statuses) {
            if (!status.terminal()) throw new IllegalArgumentException("only terminal statuses may be archived: " + status);
        }
        if (updatedBefore == null) throw new NullPointerException("updatedBefore");
        if (limit < 1 || limit > 10_000) throw new IllegalArgumentException("limit must be 1..10000");
    }

    public static ArchiveRequest completedBefore(Instant cutoff) {
        return new ArchiveRequest(
                Set.of(WorkflowStatus.COMPLETED, WorkflowStatus.FAILED, WorkflowStatus.CANCELLED),
                cutoff,
                1_000);
    }

    public ArchiveRequest withLimit(int newLimit) {
        return new ArchiveRequest(statuses, updatedBefore, newLimit);
    }
}
