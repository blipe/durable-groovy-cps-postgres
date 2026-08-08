package io.github.durablecps.observability;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;

/** Low-cardinality metrics bridge. Workflow and invocation IDs are never metric tags. */
public final class MicrometerWorkflowListener implements WorkflowListener {
    private final MeterRegistry registry;

    public MicrometerWorkflowListener(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    @Override
    public void onWorkflow(WorkflowEvent event) {
        registry.counter(
                        "durable.workflow.events",
                        "event", event.type().name().toLowerCase(),
                        "definition", event.definitionId(),
                        "status", event.status().name().toLowerCase())
                .increment();

        if (event.type() == WorkflowEventType.COMPLETED
                || event.type() == WorkflowEventType.FAILED
                || event.type() == WorkflowEventType.DEAD_LETTERED
                || event.type() == WorkflowEventType.CANCELLED) {
            registry.timer(
                            "durable.workflow.duration",
                            "definition", event.definitionId(),
                            "status", event.status().name().toLowerCase())
                    .record(event.duration());
        }
    }

    @Override
    public void onStep(StepEvent event) {
        String outcome = switch (event.type()) {
            case COMPLETED -> "success";
            case FAILED -> "failure";
            case RETRY_SCHEDULED -> "retry";
            case CANCELLED -> "cancelled";
            default -> "none";
        };
        registry.counter(
                        "durable.step.events",
                        "event", event.type().name().toLowerCase(),
                        "step", event.stepName(),
                        "outcome", outcome)
                .increment();

        if (event.type() == StepEventType.COMPLETED
                || event.type() == StepEventType.FAILED
                || event.type() == StepEventType.RETRY_SCHEDULED
                || event.type() == StepEventType.CANCELLED) {
            registry.timer(
                            "durable.step.duration",
                            "step", event.stepName(),
                            "outcome", outcome)
                    .record(event.duration());
        }
    }

    @Override
    public void onEngine(EngineEvent event) {
        registry.counter("durable.engine.events", "event", event.type().name().toLowerCase())
                .increment();
    }
}
