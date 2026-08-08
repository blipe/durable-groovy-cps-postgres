package io.github.durablecps.runtime;

/** Metadata extracted without deserializing the snapshot payload. */
public record SnapshotMetadata(
        boolean enveloped,
        int envelopeVersion,
        String codecId,
        int codecVersion,
        int payloadBytes,
        String sha256) {
    public SnapshotMetadata {
        codecId = codecId == null ? "" : codecId;
        sha256 = sha256 == null ? "" : sha256;
        if (envelopeVersion < 0) throw new IllegalArgumentException("envelopeVersion must be >= 0");
        if (codecVersion < 0) throw new IllegalArgumentException("codecVersion must be >= 0");
        if (payloadBytes < 0) throw new IllegalArgumentException("payloadBytes must be >= 0");
    }
}
