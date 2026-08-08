package io.github.durablecps.runtime;

import io.github.durablecps.api.WorkflowDefinition;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

final class WorkflowRegistry implements AutoCloseable {
    private final GroovyWorkflowCompiler compiler;
    private final ConcurrentHashMap<DefinitionKey, CompiledWorkflow> definitions = new ConcurrentHashMap<>();

    WorkflowRegistry(ClassLoader parent) {
        this.compiler = new GroovyWorkflowCompiler(parent);
    }

    synchronized void register(WorkflowDefinition definition) {
        Objects.requireNonNull(definition, "definition");
        CompiledWorkflow compiled = compiler.compile(definition);
        DefinitionKey key = new DefinitionKey(definition.id(), definition.version(), compiled.hash());

        for (DefinitionKey existing : definitions.keySet()) {
            if (existing.id.equals(definition.id()) && existing.version == definition.version()) {
                compiled.close();
                throw new IllegalStateException(
                        "workflow definition already registered for id/version: "
                                + definition.id() + "@" + definition.version());
            }
        }

        CompiledWorkflow previous = definitions.putIfAbsent(key, compiled);
        if (previous != null) {
            compiled.close();
            throw new IllegalStateException(
                    "workflow definition already registered: " + definition.id() + "@" + definition.version());
        }
    }

    CompiledWorkflow requireLatest(String id) {
        return definitions.entrySet().stream()
                .filter(entry -> entry.getKey().id.equals(id))
                .max(Comparator.comparingInt(entry -> entry.getKey().version))
                .map(java.util.Map.Entry::getValue)
                .orElseThrow(() -> new IllegalStateException("workflow definition is not registered: " + id));
    }

    CompiledWorkflow find(String id, int version, String hash) {
        return definitions.get(new DefinitionKey(id, version, hash));
    }

    boolean contains(String id, int version, String hash) {
        return definitions.containsKey(new DefinitionKey(id, version, hash));
    }

    List<CompiledWorkflow> versions(String id) {
        return definitions.entrySet().stream()
                .filter(entry -> entry.getKey().id.equals(id))
                .sorted(Comparator.comparingInt(entry -> entry.getKey().version))
                .map(java.util.Map.Entry::getValue)
                .toList();
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        List<CompiledWorkflow> compiled = new ArrayList<>(definitions.values());
        definitions.clear();
        for (CompiledWorkflow workflow : compiled) {
            try {
                workflow.close();
            } catch (RuntimeException e) {
                if (failure == null) failure = e;
                else failure.addSuppressed(e);
            }
        }
        if (failure != null) throw failure;
    }

    private record DefinitionKey(String id, int version, String hash) {
        private DefinitionKey {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(hash, "hash");
        }
    }
}
