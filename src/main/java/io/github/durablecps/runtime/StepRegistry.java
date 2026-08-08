package io.github.durablecps.runtime;

import io.github.durablecps.api.StepHandler;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

final class StepRegistry {
    private final ConcurrentHashMap<String, StepHandler> handlers = new ConcurrentHashMap<>();

    void register(String name, StepHandler handler) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(handler, "handler");
        if (name.isBlank()) throw new IllegalArgumentException("step name must not be blank");
        StepHandler previous = handlers.putIfAbsent(name, handler);
        if (previous != null) throw new IllegalStateException("step handler already registered: " + name);
    }

    StepHandler find(String name) {
        return handlers.get(name);
    }

    Map<String, StepHandler> snapshot() {
        return Map.copyOf(handlers);
    }
}
