package io.github.durablecps.observability;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public record StepEvent(
        StepEventType type,
        String instanceId,
        String workflowId,
        String invocationId,
        String stepName,
        int attempt,
        int maximumAttempts,
        Instant at,
        Instant startedAt,
        Duration duration,
        String errorType,
        String errorMessage,
        Map<String, String> context) {
    public StepEvent {
        errorType = errorType == null ? "" : errorType;
        errorMessage = errorMessage == null ? "" : errorMessage;
        context = Map.copyOf(context == null ? Map.of() : context);
    }
}
