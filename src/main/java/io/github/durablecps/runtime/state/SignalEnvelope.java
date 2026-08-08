package io.github.durablecps.runtime.state;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

public record SignalEnvelope(String name, Serializable payload, Instant receivedAt) implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

    public SignalEnvelope {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(receivedAt, "receivedAt");
    }
}
