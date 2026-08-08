package io.github.durablecps.store;

import io.github.durablecps.runtime.state.WorkflowRecord;
import java.io.Closeable;
import java.util.Collection;
import java.util.Optional;

/** A compare-and-set store. Each record replacement must be atomic. */
public interface WorkflowStore extends Closeable {
    WorkflowRecord create(WorkflowRecord initial);

    Optional<WorkflowRecord> load(String workflowId);

    WorkflowRecord replace(String workflowId, long expectedRevision, WorkflowRecord replacement);

    Collection<WorkflowRecord> list();

    @Override
    default void close() {
    }
}
