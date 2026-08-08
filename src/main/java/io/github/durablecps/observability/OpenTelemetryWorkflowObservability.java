package io.github.durablecps.observability;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Persists text-map propagation fields and creates lifecycle event spans after durable transitions.
 */
public final class OpenTelemetryWorkflowObservability
        implements WorkflowListener, WorkflowContextPropagator {
    private static final TextMapSetter<Map<String, String>> SETTER = Map::put;
    private static final TextMapGetter<Map<String, String>> GETTER =
            new TextMapGetter<>() {
                @Override
                public Iterable<String> keys(Map<String, String> carrier) {
                    return carrier.keySet();
                }

                @Override
                public String get(Map<String, String> carrier, String key) {
                    return carrier == null ? null : carrier.get(key);
                }
            };

    private final Tracer tracer;
    private final TextMapPropagator propagator;

    public OpenTelemetryWorkflowObservability(OpenTelemetry openTelemetry) {
        Objects.requireNonNull(openTelemetry, "openTelemetry");
        tracer = openTelemetry.getTracer("io.github.durablecps", "0.6.0");
        propagator = openTelemetry.getPropagators().getTextMapPropagator();
    }

    @Override
    public Map<String, String> capture() {
        LinkedHashMap<String, String> carrier = new LinkedHashMap<>();
        propagator.inject(Context.current(), carrier, SETTER);
        return Map.copyOf(carrier);
    }

    @Override
    public WorkflowContextPropagator.Scope restore(Map<String, String> context) {
        io.opentelemetry.context.Scope scope = propagator
                .extract(Context.root(), context == null ? Map.of() : context, GETTER)
                .makeCurrent();
        return scope::close;
    }

    @Override
    public void onWorkflow(WorkflowEvent event) {
        Span span = tracer.spanBuilder("durable.workflow." + event.type().name().toLowerCase())
                .setSpanKind(SpanKind.INTERNAL)
                .setParent(parent(event.context()))
                .setAttribute("durable.workflow.id", event.workflowId())
                .setAttribute("durable.workflow.definition", event.definitionId())
                .setAttribute("durable.workflow.version", event.definitionVersion())
                .setAttribute("durable.workflow.status", event.status().name())
                .setAttribute("durable.workflow.revision", event.revision())
                .startSpan();
        try {
            if (event.type() == WorkflowEventType.FAILED
                    || event.type() == WorkflowEventType.DEAD_LETTERED) {
                span.setStatus(StatusCode.ERROR, event.detail());
            }
            if (!event.detail().isEmpty()) span.addEvent(event.detail());
        } finally {
            span.end();
        }
    }

    @Override
    public void onStep(StepEvent event) {
        Span span = tracer.spanBuilder("durable.step." + event.stepName())
                .setSpanKind(SpanKind.INTERNAL)
                .setParent(parent(event.context()))
                .setAttribute("durable.workflow.id", event.workflowId())
                .setAttribute("durable.step.invocation_id", event.invocationId())
                .setAttribute("durable.step.name", event.stepName())
                .setAttribute("durable.step.event", event.type().name())
                .setAttribute("durable.step.attempt", event.attempt())
                .setAttribute("durable.step.maximum_attempts", event.maximumAttempts())
                .startSpan();
        try {
            if (!event.errorType().isEmpty()) {
                span.setAttribute("error.type", event.errorType());
                span.setStatus(StatusCode.ERROR, event.errorMessage());
            }
        } finally {
            span.end();
        }
    }

    @Override
    public void onEngine(EngineEvent event) {
        Span span = tracer.spanBuilder("durable.engine." + event.type().name().toLowerCase())
                .setSpanKind(SpanKind.INTERNAL)
                .setNoParent()
                .setAttribute("durable.engine.instance", event.instanceId())
                .startSpan();
        try {
            if (event.workflowId() != null) {
                span.setAttribute("durable.workflow.id", event.workflowId());
            }
            if (event.invocationId() != null) {
                span.setAttribute("durable.step.invocation_id", event.invocationId());
            }
            if (event.failure() != null) {
                span.recordException(event.failure());
                span.setStatus(StatusCode.ERROR, event.message());
            }
        } finally {
            span.end();
        }
    }

    private Context parent(Map<String, String> context) {
        return propagator.extract(Context.root(), context == null ? Map.of() : context, GETTER);
    }
}
