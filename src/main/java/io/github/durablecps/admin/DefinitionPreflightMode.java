package io.github.durablecps.admin;

/** Startup behavior when persisted resumable workflows cannot be matched to registered source. */
public enum DefinitionPreflightMode {
    OFF,
    WARN,
    FAIL
}
