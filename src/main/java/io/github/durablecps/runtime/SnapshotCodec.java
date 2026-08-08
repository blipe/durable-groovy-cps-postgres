package io.github.durablecps.runtime;

/** A durable object codec with an inspectable, versioned snapshot envelope. */
public interface SnapshotCodec extends ObjectCodec {
    String codecId();

    int codecVersion();

    SnapshotMetadata inspect(byte[] bytes);
}
