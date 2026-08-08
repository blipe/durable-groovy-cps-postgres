package io.github.durablecps.api;

import java.io.Serial;

/** Serializable exception injected back into the CPS script after a durable step fails. */
public final class DurableStepException extends RuntimeException {
    @Serial private static final long serialVersionUID = 1L;

    private final String remoteType;

    public DurableStepException(String remoteType, String message) {
        super(message);
        this.remoteType = remoteType == null ? "java.lang.RuntimeException" : remoteType;
    }

    public String remoteType() {
        return remoteType;
    }
}
