package io.github.durablecps.admin;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public record StuckWorkReport(
        Instant checkedAt,
        Duration overdueBy,
        List<StuckWorkflow> workflows,
        List<StuckStep> steps,
        boolean truncated) {
    public StuckWorkReport {
        if (checkedAt == null) throw new NullPointerException("checkedAt");
        if (overdueBy == null || overdueBy.isNegative()) {
            throw new IllegalArgumentException("overdueBy must not be negative");
        }
        workflows = List.copyOf(workflows == null ? List.of() : workflows);
        steps = List.copyOf(steps == null ? List.of() : steps);
    }

    public boolean empty() {
        return workflows.isEmpty() && steps.isEmpty();
    }
}
