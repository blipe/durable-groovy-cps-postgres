package io.github.durablecps.runtime;

import com.cloudbees.groovy.cps.Continuable;
import groovy.lang.Binding;
import groovy.lang.Script;
import io.github.durablecps.api.DurableCommand;
import io.github.durablecps.api.StepOptions;
import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Base class exposed to trusted Groovy workflow scripts. */
public abstract class DurableScript extends Script implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

    private LinkedHashMap<String, Serializable> input = new LinkedHashMap<>();
    private long durableSequence;

    public DurableScript() {
        super();
    }

    public DurableScript(Binding binding) {
        super(binding);
    }

    final void initializeInput(Map<String, ? extends Serializable> values) {
        this.input = values == null ? new LinkedHashMap<>() : new LinkedHashMap<>(values);
    }

    public final Map<String, Serializable> getInput() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(input));
    }

    public final Serializable input(String name) {
        return input.get(name);
    }

    public final Object step(String name) {
        return step(name, null, StepOptions.DEFAULT);
    }

    public final Object step(String name, Object argument) {
        return step(name, argument, StepOptions.DEFAULT);
    }

    public final Object step(String name, Object argument, StepOptions options) {
        Serializable serializable = requireSerializable(argument, "step argument");
        return Continuable.suspend("step", new DurableCommand.Step(nextSequence(), name, serializable, options));
    }

    public final Object sleepMillis(long milliseconds) {
        if (milliseconds < 0) throw new IllegalArgumentException("milliseconds must be >= 0");
        return Continuable.suspend(
                "sleepMillis", new DurableCommand.Sleep(nextSequence(), Duration.ofMillis(milliseconds)));
    }

    public final Object sleepFor(Duration duration) {
        return Continuable.suspend("sleepFor", new DurableCommand.Sleep(nextSequence(), duration));
    }

    public final Object awaitSignal(String name) {
        return Continuable.suspend("awaitSignal", new DurableCommand.AwaitSignal(nextSequence(), name));
    }

    private long nextSequence() {
        return ++durableSequence;
    }

    static Serializable requireSerializable(Object value, String role) {
        if (value == null) return null;
        if (value instanceof Serializable serializable) return serializable;
        throw new IllegalArgumentException(role + " must implement java.io.Serializable: " + value.getClass().getName());
    }
}
