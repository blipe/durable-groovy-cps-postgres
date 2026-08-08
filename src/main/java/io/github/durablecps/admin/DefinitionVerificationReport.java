package io.github.durablecps.admin;

import java.time.Instant;
import java.util.List;

public record DefinitionVerificationReport(
        Instant checkedAt,
        long resumableWorkflows,
        long compatibleWorkflows,
        List<DefinitionCompatibilityIssue> issues) {
    public DefinitionVerificationReport {
        issues = List.copyOf(issues == null ? List.of() : issues);
        if (resumableWorkflows < 0 || compatibleWorkflows < 0 || compatibleWorkflows > resumableWorkflows) {
            throw new IllegalArgumentException("invalid workflow counts");
        }
    }

    public boolean compatible() {
        return issues.isEmpty();
    }

    public long incompatibleWorkflows() {
        return resumableWorkflows - compatibleWorkflows;
    }
}
