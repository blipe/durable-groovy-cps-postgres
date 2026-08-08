package io.github.durablecps.store;

import io.github.durablecps.admin.WorkflowQuery;
import io.github.durablecps.api.WorkflowStatus;
import java.util.Map;

/** Optional operational query capability implemented by production stores. */
public interface SearchableWorkflowStore {
    WorkflowRecordPage search(WorkflowQuery query);

    Map<WorkflowStatus, Long> countByStatus();

    void checkHealth();
}
