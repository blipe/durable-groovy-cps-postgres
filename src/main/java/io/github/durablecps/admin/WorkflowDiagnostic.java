package io.github.durablecps.admin;

import io.github.durablecps.api.WorkflowView;
import io.github.durablecps.runtime.SnapshotMetadata;
import io.github.durablecps.runtime.state.HistoryEntry;
import io.github.durablecps.store.StepInvocationRecord;
import java.util.List;
import java.util.Optional;

/** Read-only operational detail for one active workflow. */
public record WorkflowDiagnostic(
        WorkflowView workflow,
        boolean exactDefinitionRegistered,
        Optional<SnapshotMetadata> continuationSnapshot,
        Optional<StepInvocationRecord> pendingStep,
        List<HistoryEntry> history,
        String snapshotProblem) {
    public WorkflowDiagnostic {
        continuationSnapshot = continuationSnapshot == null ? Optional.empty() : continuationSnapshot;
        pendingStep = pendingStep == null ? Optional.empty() : pendingStep;
        history = List.copyOf(history == null ? List.of() : history);
        snapshotProblem = snapshotProblem == null ? "" : snapshotProblem;
    }
}
