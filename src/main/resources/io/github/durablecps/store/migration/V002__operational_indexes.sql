create index if not exists durable_workflow_definition_idx
    on durable_workflow(definition_id, definition_version, definition_hash, status);

create index if not exists durable_workflow_updated_idx
    on durable_workflow(updated_at, workflow_id);

create index if not exists durable_step_lease_idx
    on durable_step_invocation(lease_owner, lease_expires_at, status);

create index if not exists durable_archive_updated_idx
    on durable_workflow_archive(updated_at, archive_id);
