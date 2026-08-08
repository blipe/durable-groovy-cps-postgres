package io.github.durablecps.store;

/** Raised when a node tries to commit with an expired or superseded fencing token. */
public final class LeaseLostException extends RuntimeException {
    @java.io.Serial private static final long serialVersionUID = 1L;
    public LeaseLostException(String message) {
        super(message);
    }
}
