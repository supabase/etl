-- Durable per-replication-worker progress.
--
-- `flush_lsn` uses logical replication progress-boundary semantics: all
-- source WAL before this LSN has been durably processed by ETL.

create type etl.replication_worker_type as enum (
    'apply',
    'table_sync'
);

create table etl.replication_progress (
    id bigint generated always as identity primary key,
    pipeline_id bigint not null,
    worker_type etl.replication_worker_type not null,
    table_id oid,
    flush_lsn pg_lsn not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint replication_progress_worker_table_check check (
        (worker_type = 'apply' and table_id is null)
        or
        (worker_type = 'table_sync' and table_id is not null)
    )
);

create unique index uq_replication_progress_pipeline_worker_table
    on etl.replication_progress (pipeline_id, worker_type, coalesce(table_id, 0::oid));
