package io.github.durablecps.runtime;

/** Raised when a durable snapshot envelope is malformed or its checksum does not match. */
public final class SnapshotIntegrityException extends IllegalStateException {
    public SnapshotIntegrityException(String message) {
        super(message);
    }

    public SnapshotIntegrityException(String message, Throwable cause) {
        super(message, cause);
    }
}
