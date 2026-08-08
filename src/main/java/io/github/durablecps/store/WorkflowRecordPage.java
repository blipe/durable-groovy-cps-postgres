package io.github.durablecps.store;

import io.github.durablecps.runtime.state.WorkflowRecord;
import java.util.List;

public record WorkflowRecordPage(List<WorkflowRecord> items, long total) {
    public WorkflowRecordPage {
        items = List.copyOf(items == null ? List.of() : items);
        if (total < 0) throw new IllegalArgumentException("total must be >= 0");
    }
}
