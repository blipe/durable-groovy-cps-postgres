package io.github.durablecps.runtime.state;

import io.github.durablecps.api.DurableCommand;
import io.github.durablecps.api.WorkflowStatus;
import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Entire durable state for one local workflow. Store mutations replace this immutable value. */
public record WorkflowRecord(
        String id,
        long revision,
        String definitionId,
        int definitionVersion,
        String definitionHash,
        WorkflowStatus status,
        byte[] continuation,
        DurableCommand pendingCommand,
        ResumeValue resumeValue,
        Serializable output,
        FailureData failure,
        int attempt,
        Instant availableAt,
        Instant createdAt,
        Instant updatedAt,
        Map<String, String> telemetryContext,
        List<SignalEnvelope> signals,
        List<HistoryEntry> history)
        implements Serializable {
    @Serial private static final long serialVersionUID = 1L;
    public static final int MAX_HISTORY_ENTRIES = 1_000;

    public WorkflowRecord {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(definitionId, "definitionId");
        Objects.requireNonNull(definitionHash, "definitionHash");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
        if (revision < 0) throw new IllegalArgumentException("revision must be >= 0");
        if (definitionVersion < 1) throw new IllegalArgumentException("definitionVersion must be >= 1");
        if (attempt < 0) throw new IllegalArgumentException("attempt must be >= 0");
        continuation = continuation == null ? null : continuation.clone();
        telemetryContext = Map.copyOf(telemetryContext == null ? Map.of() : telemetryContext);
        signals = List.copyOf(signals == null ? List.of() : signals);
        history = boundedHistory(history == null ? List.of() : history);
        validateShape(status, continuation, pendingCommand, resumeValue, output, failure);
    }


    private static List<HistoryEntry> boundedHistory(List<HistoryEntry> value) {
        if (value.size() <= MAX_HISTORY_ENTRIES) return List.copyOf(value);
        ArrayList<HistoryEntry> bounded = new ArrayList<>(MAX_HISTORY_ENTRIES);
        // Preserve the origin event and the newest operational history.
        bounded.add(value.get(0));
        bounded.addAll(value.subList(value.size() - (MAX_HISTORY_ENTRIES - 1), value.size()));
        return List.copyOf(bounded);
    }

    private static void validateShape(
            WorkflowStatus status,
            byte[] continuation,
            DurableCommand pendingCommand,
            ResumeValue resumeValue,
            Serializable output,
            FailureData failure) {
        if (status == WorkflowStatus.READY) {
            if (continuation == null || resumeValue == null || pendingCommand != null || output != null || failure != null) {
                throw new IllegalArgumentException(
                        "READY requires continuation+resumeValue and forbids pending/output/failure");
            }
            return;
        }
        if (status == WorkflowStatus.WAITING) {
            if (continuation == null || pendingCommand == null || resumeValue != null || output != null || failure != null) {
                throw new IllegalArgumentException(
                        "WAITING requires continuation+pendingCommand and forbids resume/output/failure");
            }
            return;
        }
        if (status == WorkflowStatus.DEAD_LETTERED) {
            if (continuation == null || failure == null || output != null) throw new IllegalArgumentException("DEAD_LETTERED requires continuation+failure and forbids output");
            if (pendingCommand != null && resumeValue != null) throw new IllegalArgumentException("DEAD_LETTERED cannot have both pendingCommand and resumeValue");
            return;
        }
        if (continuation != null || pendingCommand != null || resumeValue != null) {
            throw new IllegalArgumentException("terminal states cannot contain continuation, pending command, or resume value");
        }
        if (status == WorkflowStatus.COMPLETED && failure != null) {
            throw new IllegalArgumentException("COMPLETED cannot contain failure");
        }
        if (status == WorkflowStatus.FAILED && failure == null) {
            throw new IllegalArgumentException("FAILED requires failure");
        }
        if (status == WorkflowStatus.CANCELLED && (output != null || failure != null)) {
            throw new IllegalArgumentException("CANCELLED cannot contain output or failure");
        }
    }

    @Override
    public byte[] continuation() {
        return continuation == null ? null : continuation.clone();
    }

    public WorkflowRecord next(
            WorkflowStatus newStatus,
            byte[] newContinuation,
            DurableCommand newPendingCommand,
            ResumeValue newResumeValue,
            Serializable newOutput,
            FailureData newFailure,
            int newAttempt,
            Instant newAvailableAt,
            List<SignalEnvelope> newSignals,
            HistoryEntry event,
            Instant now) {
        List<HistoryEntry> updatedHistory = new ArrayList<>(history);
        if (event != null) updatedHistory.add(event);
        return new WorkflowRecord(
                id,
                revision + 1,
                definitionId,
                definitionVersion,
                definitionHash,
                newStatus,
                newContinuation,
                newPendingCommand,
                newResumeValue,
                newOutput,
                newFailure,
                newAttempt,
                newAvailableAt,
                createdAt,
                now,
                telemetryContext,
                newSignals == null ? signals : newSignals,
                updatedHistory);
    }

    public WorkflowRecord withSignal(SignalEnvelope signal, Instant now) {
        List<SignalEnvelope> updatedSignals = new ArrayList<>(signals);
        updatedSignals.add(signal);
        return next(
                status,
                continuation,
                pendingCommand,
                resumeValue,
                output,
                failure,
                attempt,
                availableAt,
                updatedSignals,
                new HistoryEntry(now, "SIGNAL_RECEIVED", signal.name()),
                now);
    }
}
