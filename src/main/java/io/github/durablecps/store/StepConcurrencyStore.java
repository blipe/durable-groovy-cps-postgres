package io.github.durablecps.store;

import java.util.Map;

/** Database-global step concurrency configuration shared by every engine instance. */
public interface StepConcurrencyStore {
    /**
     * Registers an immutable concurrency limit. Re-registering the same value is idempotent;
     * a different value is rejected so replicas cannot silently disagree.
     */
    void registerStepConcurrencyLimit(String stepName, int maximumConcurrent);

    Map<String, Integer> stepConcurrencyLimits();
}
