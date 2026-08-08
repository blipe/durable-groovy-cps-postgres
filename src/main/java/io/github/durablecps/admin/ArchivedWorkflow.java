package io.github.durablecps.admin;

import io.github.durablecps.runtime.state.WorkflowRecord;
import io.github.durablecps.store.StepInvocationRecord;
import java.util.List;

public record ArchivedWorkflow(
        ArchivedWorkflowSummary summary,
        WorkflowRecord workflow,
        List<StepInvocationRecord> steps) {
    public ArchivedWorkflow {
        steps = List.copyOf(steps == null ? List.of() : steps);
    }
}
