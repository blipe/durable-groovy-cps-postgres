package io.github.durablecps.store;

/** Store with checksummed, ordered schema migrations. */
public interface SchemaManagedWorkflowStore extends WorkflowStore {
    SchemaStatus schemaStatus();

    SchemaStatus migrateSchema();
}
