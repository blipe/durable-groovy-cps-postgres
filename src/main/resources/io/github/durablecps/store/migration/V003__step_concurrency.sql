create table if not exists durable_step_concurrency_limit (
    step_name varchar(200) primary key,
    max_concurrency integer not null,
    updated_at timestamptz not null,
    check (max_concurrency > 0)
);

create index if not exists durable_step_active_name_idx
    on durable_step_invocation(step_name, status, lease_expires_at, deadline);
