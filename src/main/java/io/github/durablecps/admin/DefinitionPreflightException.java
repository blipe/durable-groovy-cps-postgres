package io.github.durablecps.admin;

public final class DefinitionPreflightException extends IllegalStateException {
    private final DefinitionVerificationReport report;

    public DefinitionPreflightException(DefinitionVerificationReport report) {
        super("persisted workflows require unavailable definition versions: " + report.incompatibleWorkflows());
        this.report = report;
    }

    public DefinitionVerificationReport report() {
        return report;
    }
}
