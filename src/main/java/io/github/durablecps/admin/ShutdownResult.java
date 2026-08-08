package io.github.durablecps.admin;

import java.time.Duration;
import java.time.Instant;

/** Result of a bounded graceful engine shutdown. */
public record ShutdownResult(
        boolean drained,
        int activeStarts,
        int activePolls,
        int activeDrivers,
        int inFlightSteps,
        int activeMaintenance,
        Duration waited,
        Instant completedAt) {
    public ShutdownResult {
        waited = waited == null || waited.isNegative() ? Duration.ZERO : waited;
        if (activeStarts < 0 || activePolls < 0 || activeDrivers < 0
                || inFlightSteps < 0 || activeMaintenance < 0) {
            throw new IllegalArgumentException("active counts must be non-negative");
        }
    }

    /** Source-compatible constructor for callers compiled against the 0.5 lifecycle shape. */
    public ShutdownResult(
            boolean drained,
            int activeStarts,
            int activePolls,
            int activeDrivers,
            int inFlightSteps,
            Duration waited,
            Instant completedAt) {
        this(drained, activeStarts, activePolls, activeDrivers, inFlightSteps, 0, waited, completedAt);
    }
}
