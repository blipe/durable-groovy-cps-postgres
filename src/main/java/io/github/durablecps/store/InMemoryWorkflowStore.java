package io.github.durablecps.store;

import io.github.durablecps.api.ConcurrentWorkflowUpdateException;
import io.github.durablecps.runtime.state.WorkflowRecord;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryWorkflowStore implements WorkflowStore {
    private final ConcurrentHashMap<String, WorkflowRecord> records = new ConcurrentHashMap<>();

    @Override
    public WorkflowRecord create(WorkflowRecord initial) {
        WorkflowRecord existing = records.putIfAbsent(initial.id(), initial);
        if (existing != null) throw new IllegalStateException("workflow already exists: " + initial.id());
        return initial;
    }

    @Override
    public Optional<WorkflowRecord> load(String workflowId) {
        return Optional.ofNullable(records.get(workflowId));
    }

    @Override
    public WorkflowRecord replace(String workflowId, long expectedRevision, WorkflowRecord replacement) {
        if (!workflowId.equals(replacement.id())) {
            throw new IllegalArgumentException("replacement id does not match key");
        }
        records.compute(workflowId, (id, current) -> {
            if (current == null) throw new IllegalStateException("workflow does not exist: " + workflowId);
            if (current.revision() != expectedRevision) {
                throw new ConcurrentWorkflowUpdateException(workflowId, expectedRevision, current.revision());
            }
            if (replacement.revision() != expectedRevision + 1) {
                throw new IllegalArgumentException("replacement revision must be expectedRevision + 1");
            }
            return replacement;
        });
        return replacement;
    }

    @Override
    public Collection<WorkflowRecord> list() {
        return new ArrayList<>(records.values());
    }
}
