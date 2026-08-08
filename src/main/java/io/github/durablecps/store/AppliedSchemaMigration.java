package io.github.durablecps.store;

import java.time.Instant;

public record AppliedSchemaMigration(
        int version,
        String description,
        String checksum,
        Instant installedAt) {
    public AppliedSchemaMigration {
        if (version < 1) throw new IllegalArgumentException("version must be positive");
        description = description == null ? "" : description;
        checksum = checksum == null ? "" : checksum;
    }
}
