package io.github.durablecps.store;

import java.util.List;

/** Versioned store-schema status used by startup preflight and operations. */
public record SchemaStatus(
        int currentVersion,
        int targetVersion,
        boolean compatible,
        List<AppliedSchemaMigration> applied,
        List<Integer> pendingVersions,
        String message) {
    public SchemaStatus {
        if (currentVersion < 0 || targetVersion < 0) {
            throw new IllegalArgumentException("schema versions must be non-negative");
        }
        applied = List.copyOf(applied == null ? List.of() : applied);
        pendingVersions = List.copyOf(pendingVersions == null ? List.of() : pendingVersions);
        message = message == null ? "" : message;
    }
}
