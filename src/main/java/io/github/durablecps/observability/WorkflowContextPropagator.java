package io.github.durablecps.observability;

import java.util.Map;

/** Captures serializable propagation fields and restores them around CPS and step execution. */
public interface WorkflowContextPropagator {
    WorkflowContextPropagator NONE = new WorkflowContextPropagator() {
        @Override
        public Map<String, String> capture() {
            return Map.of();
        }

        @Override
        public Scope restore(Map<String, String> context) {
            return Scope.NONE;
        }
    };

    Map<String, String> capture();

    Scope restore(Map<String, String> context);

    interface Scope extends AutoCloseable {
        Scope NONE = () -> {};

        @Override
        void close();
    }
}
