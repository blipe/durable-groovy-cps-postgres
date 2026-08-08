package io.github.durablecps.api;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface StepHandler {
    /**
     * Execute at least once. Implementations should deduplicate on request.invocationId().
     */
    CompletionStage<? extends Serializable> execute(StepRequest request, StepContext context);
}
