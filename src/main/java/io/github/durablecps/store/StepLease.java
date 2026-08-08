package io.github.durablecps.store;

import java.time.Instant;
import java.util.Objects;

/** Fenced ownership of one durable step invocation. */
public record StepLease(String invocationId, String owner, long token, Instant expiresAt) {
    public StepLease {
        Objects.requireNonNull(invocationId, "invocationId");
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(expiresAt, "expiresAt");
        if (token < 1) throw new IllegalArgumentException("token must be >= 1");
    }
}
