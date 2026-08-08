package io.github.durablecps.observability;

/** Receives immutable lifecycle events after the corresponding durable state transition commits. */
public interface WorkflowListener {
    default void onWorkflow(WorkflowEvent event) {}

    default void onStep(StepEvent event) {}

    default void onEngine(EngineEvent event) {}
}
