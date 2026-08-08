package io.github.durablecps.store;

import java.util.Objects;

public record ClaimedStep(StepInvocationRecord invocation, StepLease lease) {
    public ClaimedStep {
        Objects.requireNonNull(invocation, "invocation");
        Objects.requireNonNull(lease, "lease");
    }
}
