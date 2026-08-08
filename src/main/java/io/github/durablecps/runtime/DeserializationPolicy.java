package io.github.durablecps.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Limits and class-admission rules for trusted durable Java-serialization snapshots. */
public final class DeserializationPolicy {
    private final int maximumBytes;
    private final long maximumDepth;
    private final long maximumReferences;
    private final long maximumArrayLength;
    private final List<String> allowedPackagePrefixes;
    private final Set<String> allowedClassNames;
    private final boolean allowGeneratedWorkflowClasses;
    private final boolean allowDynamicProxies;

    private DeserializationPolicy(Builder builder) {
        maximumBytes = builder.maximumBytes;
        maximumDepth = builder.maximumDepth;
        maximumReferences = builder.maximumReferences;
        maximumArrayLength = builder.maximumArrayLength;
        allowedPackagePrefixes = List.copyOf(builder.allowedPackagePrefixes);
        allowedClassNames = Set.copyOf(builder.allowedClassNames);
        allowGeneratedWorkflowClasses = builder.allowGeneratedWorkflowClasses;
        allowDynamicProxies = builder.allowDynamicProxies;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Production-oriented defaults for the engine, Groovy CPS, and JDK collection graph. */
    public static DeserializationPolicy productionDefaults() {
        return builder()
                .allowPackage("java.")
                .allowPackage("javax.")
                .allowPackage("jdk.")
                .allowPackage("sun.util.")
                .allowPackage("groovy.")
                .allowPackage("org.codehaus.groovy.")
                .allowPackage("org.kohsuke.groovy.sandbox.")
                .allowPackage("com.cloudbees.groovy.cps.")
                .allowPackage("com.google.common.")
                .allowPackage("io.github.durablecps.")
                .allowGeneratedWorkflowClasses(true)
                .build();
    }

    public int maximumBytes() {
        return maximumBytes;
    }

    public long maximumDepth() {
        return maximumDepth;
    }

    public long maximumReferences() {
        return maximumReferences;
    }

    public long maximumArrayLength() {
        return maximumArrayLength;
    }

    public boolean allowDynamicProxies() {
        return allowDynamicProxies;
    }

    public boolean allowsClassName(String rawName) {
        Objects.requireNonNull(rawName, "rawName");
        String name = componentName(rawName);
        if (primitiveName(name)) return true;
        if (allowedClassNames.contains(name)) return true;
        if (allowGeneratedWorkflowClasses && name.startsWith("Durable_") && name.indexOf('.') < 0) return true;
        for (String prefix : allowedPackagePrefixes) {
            if (name.startsWith(prefix)) return true;
        }
        return false;
    }

    private static String componentName(String name) {
        while (name.startsWith("[")) name = name.substring(1);
        if (name.length() == 1) {
            return switch (name.charAt(0)) {
                case 'Z' -> "boolean";
                case 'B' -> "byte";
                case 'C' -> "char";
                case 'S' -> "short";
                case 'I' -> "int";
                case 'J' -> "long";
                case 'F' -> "float";
                case 'D' -> "double";
                default -> name;
            };
        }
        if (name.startsWith("L") && name.endsWith(";")) return name.substring(1, name.length() - 1);
        return name;
    }

    private static boolean primitiveName(String name) {
        return switch (name) {
            case "boolean", "byte", "char", "short", "int", "long", "float", "double", "void" -> true;
            default -> false;
        };
    }

    public static final class Builder {
        private int maximumBytes = 64 * 1024 * 1024;
        private long maximumDepth = 256;
        private long maximumReferences = 1_000_000;
        private long maximumArrayLength = 1_000_000;
        private final List<String> allowedPackagePrefixes = new ArrayList<>();
        private final List<String> allowedClassNames = new ArrayList<>();
        private boolean allowGeneratedWorkflowClasses;
        private boolean allowDynamicProxies;

        public Builder maximumBytes(int value) {
            if (value < 1024) throw new IllegalArgumentException("maximumBytes must be >= 1024");
            maximumBytes = value;
            return this;
        }

        public Builder maximumDepth(long value) {
            if (value < 1) throw new IllegalArgumentException("maximumDepth must be >= 1");
            maximumDepth = value;
            return this;
        }

        public Builder maximumReferences(long value) {
            if (value < 1) throw new IllegalArgumentException("maximumReferences must be >= 1");
            maximumReferences = value;
            return this;
        }

        public Builder maximumArrayLength(long value) {
            if (value < 0) throw new IllegalArgumentException("maximumArrayLength must be >= 0");
            maximumArrayLength = value;
            return this;
        }

        /** Prefix must include its package separator, for example {@code com.acme.orders.}. */
        public Builder allowPackage(String prefix) {
            Objects.requireNonNull(prefix, "prefix");
            if (prefix.isBlank() || !prefix.endsWith(".")) {
                throw new IllegalArgumentException("package prefix must be nonblank and end with '.'");
            }
            allowedPackagePrefixes.add(prefix);
            return this;
        }

        public Builder allowClass(String className) {
            Objects.requireNonNull(className, "className");
            if (className.isBlank()) throw new IllegalArgumentException("className must not be blank");
            allowedClassNames.add(className);
            return this;
        }

        public Builder allowGeneratedWorkflowClasses(boolean value) {
            allowGeneratedWorkflowClasses = value;
            return this;
        }

        public Builder allowDynamicProxies(boolean value) {
            allowDynamicProxies = value;
            return this;
        }

        public DeserializationPolicy build() {
            return new DeserializationPolicy(this);
        }
    }
}
