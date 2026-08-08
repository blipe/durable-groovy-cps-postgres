package io.github.durablecps.api;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/** Retry and execution policy for one durable step. */
public record StepOptions(
        int maxAttempts,
        Duration initialBackoff,
        double backoffMultiplier,
        Duration executionTimeout)
        implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

    public static final Duration DEFAULT_EXECUTION_TIMEOUT = Duration.ofMinutes(5);
    public static final StepOptions DEFAULT =
            new StepOptions(1, Duration.ZERO, 2.0d, DEFAULT_EXECUTION_TIMEOUT);

    /** Source-compatible constructor retained for callers of the 0.1 API. */
    public StepOptions(int maxAttempts, Duration initialBackoff, double backoffMultiplier) {
        this(maxAttempts, initialBackoff, backoffMultiplier, DEFAULT_EXECUTION_TIMEOUT);
    }

    public StepOptions {
        Objects.requireNonNull(initialBackoff, "initialBackoff");
        executionTimeout = executionTimeout == null ? DEFAULT_EXECUTION_TIMEOUT : executionTimeout;
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
        if (initialBackoff.isNegative()) throw new IllegalArgumentException("initialBackoff must not be negative");
        if (executionTimeout.isZero() || executionTimeout.isNegative()) {
            throw new IllegalArgumentException("executionTimeout must be positive");
        }
        if (!Double.isFinite(backoffMultiplier) || backoffMultiplier < 1.0d) {
            throw new IllegalArgumentException("backoffMultiplier must be finite and >= 1.0");
        }
    }

    public static StepOptions retry(int maxAttempts, Duration initialBackoff) {
        return new StepOptions(maxAttempts, initialBackoff, 2.0d, DEFAULT_EXECUTION_TIMEOUT);
    }

    public static StepOptions retry(
            int maxAttempts, Duration initialBackoff, Duration executionTimeout) {
        return new StepOptions(maxAttempts, initialBackoff, 2.0d, executionTimeout);
    }

    public StepOptions withExecutionTimeout(Duration timeout) {
        return new StepOptions(maxAttempts, initialBackoff, backoffMultiplier, timeout);
    }

    public Duration backoffForAttempt(int completedAttempts) {
        if (completedAttempts <= 0 || initialBackoff.isZero()) return Duration.ZERO;
        double factor = Math.pow(backoffMultiplier, completedAttempts - 1L);
        double millis = initialBackoff.toMillis() * factor;
        long bounded = millis >= Long.MAX_VALUE ? Long.MAX_VALUE : (long) millis;
        return Duration.ofMillis(bounded);
    }
}
