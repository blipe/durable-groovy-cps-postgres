package io.github.durablecps.observability;

import io.github.durablecps.api.WorkflowStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public record WorkflowEvent(
        WorkflowEventType type,
        String instanceId,
        String workflowId,
        String definitionId,
        int definitionVersion,
        WorkflowStatus previousStatus,
        WorkflowStatus status,
        long revision,
        Instant at,
        Instant createdAt,
        Duration duration,
        String detail,
        Map<String, String> context) {
    public WorkflowEvent {
        detail = detail == null ? "" : detail;
        context = Map.copyOf(context == null ? Map.of() : context);
    }
}
