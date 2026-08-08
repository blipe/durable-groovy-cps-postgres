package io.github.durablecps.admin;

import io.github.durablecps.store.SchemaStatus;

public final class SchemaPreflightException extends IllegalStateException {
    private final SchemaStatus status;

    public SchemaPreflightException(SchemaStatus status) {
        super("durable workflow schema is incompatible: " + status.message());
        this.status = status;
    }

    public SchemaStatus status() {
        return status;
    }
}
