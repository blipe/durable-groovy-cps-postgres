package io.github.durablecps.observability;

import java.time.Instant;

public record EngineEvent(
        EngineEventType type,
        String instanceId,
        String workflowId,
        String invocationId,
        Instant at,
        String message,
        Throwable failure) {
    public EngineEvent {
        message = message == null ? "" : message;
    }
}
