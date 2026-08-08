package io.github.durablecps.api;

import java.time.Clock;
import java.util.concurrent.Executor;

public interface StepContext {
    Clock clock();
    Executor executor();
    boolean isCancelled();
}
