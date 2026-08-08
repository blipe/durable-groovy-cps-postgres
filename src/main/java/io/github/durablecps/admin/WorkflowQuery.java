package io.github.durablecps.admin;

import io.github.durablecps.api.WorkflowStatus;
import java.time.Instant;
import java.util.Set;

/** Bounded administrative query. Empty status set means all statuses. */
public record WorkflowQuery(
        Set<WorkflowStatus> statuses,
        String definitionId,
        Instant createdFrom,
        Instant createdTo,
        Instant updatedFrom,
        Instant updatedTo,
        int limit,
        int offset) {

    public WorkflowQuery {
        statuses = Set.copyOf(statuses == null ? Set.of() : statuses);
        definitionId = normalize(definitionId);
        if (createdFrom != null && createdTo != null && createdFrom.isAfter(createdTo)) {
            throw new IllegalArgumentException("createdFrom must be <= createdTo");
        }
        if (updatedFrom != null && updatedTo != null && updatedFrom.isAfter(updatedTo)) {
            throw new IllegalArgumentException("updatedFrom must be <= updatedTo");
        }
        if (limit < 1 || limit > 10_000) throw new IllegalArgumentException("limit must be 1..10000");
        if (offset < 0) throw new IllegalArgumentException("offset must be >= 0");
    }

    public static WorkflowQuery all() {
        return new WorkflowQuery(Set.of(), null, null, null, null, null, 100, 0);
    }

    public WorkflowQuery withStatuses(Set<WorkflowStatus> values) {
        return new WorkflowQuery(values, definitionId, createdFrom, createdTo, updatedFrom, updatedTo, limit, offset);
    }

    public WorkflowQuery withDefinition(String value) {
        return new WorkflowQuery(statuses, value, createdFrom, createdTo, updatedFrom, updatedTo, limit, offset);
    }

    public WorkflowQuery withPage(int newLimit, int newOffset) {
        return new WorkflowQuery(statuses, definitionId, createdFrom, createdTo, updatedFrom, updatedTo, newLimit, newOffset);
    }

    private static String normalize(String value) {
        if (value == null) return null;
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
