-- Base schema for etl-replicator store (applied on the source DB)

-- Ensure etl schema exists (also set by runtime, but safe here)
create schema if not exists etl;

-- Enum for table replication state
create type etl.table_state as enum (
    'init',
    'data_sync',
    'finished_copy',
    'sync_done',
    'ready',
    'errored'
);

-- Replication state
create table etl.replication_state (
    id bigint generated always as identity primary key,
    pipeline_id bigint not null,
    table_id oid not null,
    state etl.table_state not null,
    metadata jsonb,
    prev bigint references etl.replication_state(id),
    is_current boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

-- Ensures that there is only one current state per pipeline/table
create unique index uq_replication_state_current_true
    on etl.replication_state (pipeline_id, table_id)
    where is_current = true;

-- Table schemas (per pipeline, per table)
create table etl.table_schemas (
    id bigint generated always as identity primary key,
    pipeline_id bigint not null,
    table_id oid not null,
    schema_name text not null,
    table_name text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (pipeline_id, table_id)
);

create index idx_table_schemas_pipeline_table
    on etl.table_schemas (pipeline_id, table_id);

-- Columns for stored schemas
create table etl.table_columns (
    id bigint generated always as identity primary key,
    table_schema_id bigint not null references etl.table_schemas(id) on delete cascade,
    column_name text not null,
    column_type text not null,
    type_modifier integer not null,
    nullable boolean not null,
    primary_key boolean not null,
    column_order integer not null,
    created_at timestamptz not null default now(),
    unique (table_schema_id, column_name),
    unique (table_schema_id, column_order)
);

create index idx_table_columns_order
    on etl.table_columns (table_schema_id);

-- Source-to-destination table id mappings
create table etl.table_mappings (
    id bigint generated always as identity primary key,
    pipeline_id bigint not null,
    source_table_id oid not null,
    destination_table_id text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (pipeline_id, source_table_id)
);
