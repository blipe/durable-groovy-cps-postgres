package io.github.durablecps.api;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

public record StepRequest(
        String workflowId,
        String invocationId,
        long sequence,
        String stepName,
        Serializable argument,
        int attempt,
        Instant scheduledAt,
        Instant deadline)
        implements Serializable {
    @Serial private static final long serialVersionUID = 2L;

    /** Source-compatible constructor retained for callers of the 0.1 API. */
    public StepRequest(
            String workflowId,
            String invocationId,
            String stepName,
            Serializable argument,
            int attempt,
            Instant scheduledAt) {
        this(workflowId, invocationId, parseSequence(invocationId), stepName, argument, attempt, scheduledAt, null);
    }

    public StepRequest {
        Objects.requireNonNull(workflowId, "workflowId");
        Objects.requireNonNull(invocationId, "invocationId");
        Objects.requireNonNull(stepName, "stepName");
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        if (sequence < 1) throw new IllegalArgumentException("sequence must be >= 1");
        if (attempt < 1) throw new IllegalArgumentException("attempt must be >= 1");
    }

    private static long parseSequence(String invocationId) {
        int separator = invocationId == null ? -1 : invocationId.lastIndexOf(':');
        if (separator < 0 || separator == invocationId.length() - 1) return 1L;
        try {
            return Long.parseLong(invocationId.substring(separator + 1));
        } catch (NumberFormatException ignored) {
            return 1L;
        }
    }
}
