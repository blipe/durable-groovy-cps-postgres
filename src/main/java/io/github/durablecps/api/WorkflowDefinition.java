package io.github.durablecps.api;

import java.util.Objects;

/** Immutable registered workflow source. Version changes must be explicit. */
public record WorkflowDefinition(String id, int version, String source) {
    public WorkflowDefinition {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(source, "source");
        if (id.isBlank()) throw new IllegalArgumentException("definition id must not be blank");
        if (!id.matches("[A-Za-z0-9_.-]+")) {
            throw new IllegalArgumentException("definition id must match [A-Za-z0-9_.-]+");
        }
        if (version < 1) throw new IllegalArgumentException("version must be >= 1");
    }
}
