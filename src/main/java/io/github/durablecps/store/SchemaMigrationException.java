package io.github.durablecps.store;

public final class SchemaMigrationException extends IllegalStateException {
    public SchemaMigrationException(String message) {
        super(message);
    }

    public SchemaMigrationException(String message, Throwable cause) {
        super(message, cause);
    }
}
