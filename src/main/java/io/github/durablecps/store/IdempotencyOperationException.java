package io.github.durablecps.store;

public final class IdempotencyOperationException extends RuntimeException {
    @java.io.Serial private static final long serialVersionUID = 1L;
    public IdempotencyOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
