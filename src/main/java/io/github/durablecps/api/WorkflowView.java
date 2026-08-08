package io.github.durablecps.api;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

public record WorkflowView(
        String id,
        String definitionId,
        int definitionVersion,
        WorkflowStatus status,
        long revision,
        Serializable output,
        String failureType,
        String failureMessage,
        String waitingOn,
        int attempt,
        Instant availableAt,
        int queuedSignals,
        boolean recoverable,
        Instant createdAt,
        Instant updatedAt,
        List<String> history) {
    public WorkflowView {
        history = List.copyOf(history == null ? List.of() : history);
    }
}
