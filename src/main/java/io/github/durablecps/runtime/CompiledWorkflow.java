package io.github.durablecps.runtime;

import com.cloudbees.groovy.cps.Continuable;
import io.github.durablecps.api.WorkflowDefinition;
import java.io.Serializable;
import java.util.Map;

final class CompiledWorkflow implements AutoCloseable {
    private final WorkflowDefinition definition;
    private final String hash;
    private final groovy.lang.GroovyClassLoader classLoader;
    private final Class<? extends DurableScript> scriptClass;

    CompiledWorkflow(
            WorkflowDefinition definition,
            String hash,
            groovy.lang.GroovyClassLoader classLoader,
            Class<? extends DurableScript> scriptClass) {
        this.definition = definition;
        this.hash = hash;
        this.classLoader = classLoader;
        this.scriptClass = scriptClass;
    }

    WorkflowDefinition definition() {
        return definition;
    }

    String hash() {
        return hash;
    }

    ClassLoader classLoader() {
        return classLoader;
    }

    Continuable newContinuable(Map<String, ? extends Serializable> input) {
        try {
            DurableScript script = scriptClass.getDeclaredConstructor().newInstance();
            script.initializeInput(input);
            return new Continuable(script);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot instantiate workflow script " + definition.id(), e);
        }
    }

    @Override
    public void close() {
        try {
            classLoader.close();
        } catch (java.io.IOException e) {
            throw new IllegalStateException("cannot close Groovy class loader for " + definition.id(), e);
        }
    }
}
