package io.github.durablecps.runtime.state;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

public record HistoryEntry(Instant at, String type, String detail) implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

    public HistoryEntry {
        Objects.requireNonNull(at, "at");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(detail, "detail");
    }

    @Override
    public String toString() {
        return at + " " + type + " " + detail;
    }
}
