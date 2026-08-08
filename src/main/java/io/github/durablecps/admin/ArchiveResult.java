package io.github.durablecps.admin;

import java.time.Instant;
import java.util.List;

public record ArchiveResult(Instant completedAt, List<ArchivedWorkflowSummary> archived) {
    public ArchiveResult {
        archived = List.copyOf(archived == null ? List.of() : archived);
    }

    public int count() {
        return archived.size();
    }
}
