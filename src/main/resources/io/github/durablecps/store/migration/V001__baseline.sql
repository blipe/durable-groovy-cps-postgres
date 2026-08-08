create table if not exists durable_workflow (
    workflow_id varchar(200) primary key,
    revision bigint not null,
    definition_id varchar(200) not null,
    definition_version integer not null,
    definition_hash varchar(64) not null,
    status varchar(32) not null,
    wait_kind varchar(32),
    wait_key varchar(300),
    available_at timestamptz,
    record_blob bytea not null,
    lease_owner varchar(200),
    lease_token bigint not null default 0,
    lease_expires_at timestamptz,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create table if not exists durable_signal (
    signal_id varchar(36) primary key,
    workflow_id varchar(200) not null references durable_workflow(workflow_id) on delete cascade,
    signal_position integer not null,
    signal_name varchar(200) not null,
    payload_blob bytea,
    received_at timestamptz not null,
    unique (workflow_id, signal_position)
);

create table if not exists durable_step_invocation (
    invocation_id varchar(300) primary key,
    workflow_id varchar(200) not null references durable_workflow(workflow_id) on delete cascade,
    sequence_number bigint not null,
    step_name varchar(200) not null,
    status varchar(32) not null,
    attempt integer not null,
    maximum_attempts integer not null,
    available_at timestamptz not null,
    started_at timestamptz,
    deadline timestamptz,
    record_blob bytea not null,
    lease_owner varchar(200),
    lease_token bigint not null default 0,
    lease_expires_at timestamptz,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    unique (workflow_id, sequence_number)
);


create table if not exists durable_workflow_archive (
    archive_id varchar(36) primary key,
    workflow_id varchar(200) not null,
    definition_id varchar(200) not null,
    definition_version integer not null,
    definition_hash varchar(64) not null,
    final_status varchar(32) not null,
    final_revision bigint not null,
    workflow_blob bytea not null,
    steps_blob bytea not null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    archived_at timestamptz not null
);

create index if not exists durable_workflow_archive_workflow_idx
    on durable_workflow_archive(workflow_id, archived_at);
create index if not exists durable_workflow_archive_status_idx
    on durable_workflow_archive(final_status, archived_at);

create table if not exists durable_idempotency (
    invocation_id varchar(300) primary key,
    completed boolean not null default false,
    result_is_null boolean not null default false,
    result_blob bytea,
    created_at timestamptz not null,
    completed_at timestamptz
);

create index if not exists durable_workflow_ready_idx
    on durable_workflow(status, available_at);
create index if not exists durable_workflow_lease_idx
    on durable_workflow(lease_expires_at);
create index if not exists durable_workflow_wait_idx
    on durable_workflow(wait_kind, wait_key, available_at);
create index if not exists durable_step_due_idx
    on durable_step_invocation(status, available_at, lease_expires_at);
create index if not exists durable_step_workflow_idx
    on durable_step_invocation(workflow_id, sequence_number);
create index if not exists durable_signal_workflow_idx
    on durable_signal(workflow_id, signal_name, signal_position);
