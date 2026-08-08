package io.github.durablecps.runtime;

import com.cloudbees.groovy.cps.CpsTransformer;
import groovy.lang.GroovyClassLoader;
import io.github.durablecps.api.WorkflowDefinition;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.codehaus.groovy.control.CompilerConfiguration;

final class GroovyWorkflowCompiler {
    private final ClassLoader parent;

    GroovyWorkflowCompiler(ClassLoader parent) {
        this.parent = parent;
    }

    CompiledWorkflow compile(WorkflowDefinition definition) {
        String hash = hash(definition);
        CompilerConfiguration configuration = new CompilerConfiguration();
        configuration.setScriptBaseClass(DurableScript.class.getName());
        configuration.addCompilationCustomizers(new CpsTransformer());

        GroovyClassLoader loader = new GroovyClassLoader(parent, configuration, true);
        String fileName = "Durable_" + definition.id().replaceAll("[^A-Za-z0-9_]", "_")
                + "_v" + definition.version() + "_" + hash.substring(0, 12) + ".groovy";
        Class<?> raw = loader.parseClass(definition.source(), fileName);
        if (!DurableScript.class.isAssignableFrom(raw)) {
            try {
                loader.close();
            } catch (java.io.IOException ignored) {
            }
            throw new IllegalStateException("compiled script does not extend " + DurableScript.class.getName());
        }
        @SuppressWarnings("unchecked")
        Class<? extends DurableScript> scriptClass = (Class<? extends DurableScript>) raw;
        return new CompiledWorkflow(definition, hash, loader, scriptClass);
    }

    static String hash(WorkflowDefinition definition) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(
                    digest.digest(definition.source().getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("SHA-256 unavailable", e);
        }
    }
}
