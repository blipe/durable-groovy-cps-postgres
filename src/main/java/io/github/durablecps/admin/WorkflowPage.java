package io.github.durablecps.admin;

import io.github.durablecps.api.WorkflowView;
import java.util.List;

public record WorkflowPage(List<WorkflowView> items, long total, int limit, int offset) {
    public WorkflowPage {
        items = List.copyOf(items == null ? List.of() : items);
        if (total < 0) throw new IllegalArgumentException("total must be >= 0");
        if (limit < 1) throw new IllegalArgumentException("limit must be >= 1");
        if (offset < 0) throw new IllegalArgumentException("offset must be >= 0");
    }
}
