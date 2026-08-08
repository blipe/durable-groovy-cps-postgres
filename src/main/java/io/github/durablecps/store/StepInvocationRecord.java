package io.github.durablecps.store;

import io.github.durablecps.api.StepOptions;
import io.github.durablecps.runtime.state.FailureData;
import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/** Durable, independently claimable representation of one workflow step. */
public record StepInvocationRecord(
        String invocationId,
        String workflowId,
        long sequence,
        String stepName,
        Serializable argument,
        StepOptions options,
        StepInvocationStatus status,
        int attempt,
        Instant availableAt,
        Instant startedAt,
        Instant deadline,
        Serializable result,
        FailureData failure,
        Instant createdAt,
        Instant updatedAt)
        implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

    public StepInvocationRecord {
        Objects.requireNonNull(invocationId, "invocationId");
        Objects.requireNonNull(workflowId, "workflowId");
        Objects.requireNonNull(stepName, "stepName");
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(availableAt, "availableAt");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
        if (sequence < 1) throw new IllegalArgumentException("sequence must be >= 1");
        if (attempt < 0) throw new IllegalArgumentException("attempt must be >= 0");
        if (status == StepInvocationStatus.SUCCEEDED && failure != null) {
            throw new IllegalArgumentException("SUCCEEDED step cannot contain failure");
        }
        if (status == StepInvocationStatus.FAILED && failure == null) {
            throw new IllegalArgumentException("FAILED step requires failure");
        }
        if (!status.terminal() && (result != null || failure != null)) {
            throw new IllegalArgumentException("non-terminal step cannot contain result or failure");
        }
    }

    public static StepInvocationRecord pending(
            String invocationId,
            String workflowId,
            long sequence,
            String stepName,
            Serializable argument,
            StepOptions options,
            Instant now) {
        return new StepInvocationRecord(
                invocationId,
                workflowId,
                sequence,
                stepName,
                argument,
                options,
                StepInvocationStatus.PENDING,
                0,
                now,
                null,
                null,
                null,
                null,
                now,
                now);
    }

    public StepInvocationRecord requeued(Instant now) {
        return new StepInvocationRecord(invocationId, workflowId, sequence, stepName, argument, options,
                StepInvocationStatus.PENDING, 0, now, null, null, null, null, createdAt, now);
    }

    public StepInvocationRecord running(int newAttempt, Instant now, Instant newDeadline) {
        return new StepInvocationRecord(
                invocationId,
                workflowId,
                sequence,
                stepName,
                argument,
                options,
                StepInvocationStatus.RUNNING,
                newAttempt,
                availableAt,
                now,
                newDeadline,
                null,
                null,
                createdAt,
                now);
    }

    public StepInvocationRecord retry(Instant due, Instant now) {
        return new StepInvocationRecord(
                invocationId,
                workflowId,
                sequence,
                stepName,
                argument,
                options,
                StepInvocationStatus.RETRY_WAIT,
                attempt,
                due,
                null,
                null,
                null,
                null,
                createdAt,
                now);
    }

    public StepInvocationRecord succeeded(Serializable value, Instant now) {
        return new StepInvocationRecord(
                invocationId,
                workflowId,
                sequence,
                stepName,
                argument,
                options,
                StepInvocationStatus.SUCCEEDED,
                attempt,
                availableAt,
                startedAt,
                deadline,
                value,
                null,
                createdAt,
                now);
    }

    public StepInvocationRecord cancelled(Instant now) {
        return new StepInvocationRecord(
                invocationId,
                workflowId,
                sequence,
                stepName,
                argument,
                options,
                StepInvocationStatus.CANCELLED,
                attempt,
                availableAt,
                startedAt,
                deadline,
                null,
                null,
                createdAt,
                now);
    }

    public StepInvocationRecord failed(FailureData value, Instant now) {
        return new StepInvocationRecord(
                invocationId,
                workflowId,
                sequence,
                stepName,
                argument,
                options,
                StepInvocationStatus.FAILED,
                attempt,
                availableAt,
                startedAt,
                deadline,
                null,
                value,
                createdAt,
                now);
    }
}
