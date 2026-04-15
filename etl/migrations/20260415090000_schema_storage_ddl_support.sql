-- Storage changes for DDL-aware schema tracking.
--
-- This is an intentional one-way upgrade for this branch:
-- - `etl.table_columns.column_order` is renamed to `ordinal_position`.
-- - existing stored values are shifted by `+ 1` to match PostgreSQL's 1-based ordinals.
-- - no backward-compatibility layer is kept for older pipelines.
--
-- All pipelines must be upgraded together with code that reads `ordinal_position`.

alter table etl.table_columns
    rename column column_order to ordinal_position;

-- Shift through a temporary offset first so the existing unique constraint on
-- (table_schema_id, ordinal_position) does not see transient collisions.
update etl.table_columns
set ordinal_position = ordinal_position + 1000000;

update etl.table_columns
set ordinal_position = ordinal_position - 999999;

alter table etl.table_columns
    add column if not exists primary_key_ordinal_position pg_catalog.int4;

-- Backfill primary_key_ordinal_position by querying the actual PK constraint from pg_constraint.
-- This assumes the source tables still exist and their PK order has not changed.
update etl.table_columns tc
set primary_key_ordinal_position = pk_info.pk_position
from (
    select
        tc_inner.id as column_id,
        x.n as pk_position
    from etl.table_columns tc_inner
    join etl.table_schemas ts on ts.id = tc_inner.table_schema_id
    join pg_catalog.pg_constraint con on con.conrelid = ts.table_id and con.contype = 'p'
    join pg_catalog.pg_attribute a on a.attrelid = ts.table_id and a.attname = tc_inner.column_name
    cross join lateral unnest(con.conkey) with ordinality as x(attnum, n)
    where x.attnum = a.attnum
      and tc_inner.primary_key = true
) pk_info
where tc.id = pk_info.column_id
  and tc.primary_key_ordinal_position is null;

-- Add snapshot_id column to table_schemas for schema versioning.
-- The snapshot_id value is the start_lsn of the DDL message that created this schema version.
-- Initial schemas use snapshot_id = '0/0'.
alter table etl.table_schemas
    add column if not exists snapshot_id pg_lsn not null default '0/0';

-- Change unique constraint from (pipeline_id, table_id) to (pipeline_id, table_id, snapshot_id)
-- to allow multiple schema versions per table.
alter table etl.table_schemas
    drop constraint if exists table_schemas_pipeline_id_table_id_key;

alter table etl.table_schemas
    add constraint table_schemas_pipeline_id_table_id_snapshot_id_key
    unique (pipeline_id, table_id, snapshot_id);

-- Index for efficient "find largest snapshot_id <= X" queries.
create index if not exists idx_table_schemas_pipeline_table_snapshot_id
    on etl.table_schemas (pipeline_id, table_id, snapshot_id desc);

-- Unified destination table metadata.
--
-- Tracks all destination-related state for each replicated table in a single row.
create type etl.destination_table_schema_status as enum (
    'applying',
    'applied'
);

create table etl.destination_tables_metadata (
    id bigint generated always as identity primary key,
    pipeline_id bigint not null,
    table_id oid not null,
    -- The name/identifier of the table in the destination system.
    destination_table_id text not null,
    -- The snapshot_id of the schema currently applied at the destination.
    snapshot_id pg_lsn not null,
    -- The schema version before the current change. null for initial schemas.
    -- Destinations that support atomic DDL can use this for recovery by rolling back
    -- to the previous snapshot when schema_status is 'applying' on startup.
    previous_snapshot_id pg_lsn,
    -- Status: 'applying' when a schema change is in progress, 'applied' when complete.
    -- If 'applying' is found on startup, recovery may be needed.
    schema_status etl.destination_table_schema_status not null,
    -- The replication mask as a byte array where each byte is 0 (not replicated) or 1 (replicated).
    -- The index corresponds to the column's ordinal position in the schema.
    replication_mask bytea not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    -- One metadata row per table per pipeline.
    unique (pipeline_id, table_id)
);

-- Backfill destination_tables_metadata from existing table_mappings and table_schemas.
-- For each mapped table, create metadata with snapshot_id = 0/0 (initial schema)
-- and all columns marked as replicated.
insert into etl.destination_tables_metadata (
    pipeline_id,
    table_id,
    destination_table_id,
    snapshot_id,
    schema_status,
    replication_mask
)
select
    tm.pipeline_id,
    tm.source_table_id as table_id,
    tm.destination_table_id,
    '0/0'::pg_lsn as snapshot_id,
    'applied'::etl.destination_table_schema_status,
    -- Create a bytea of 1s with length equal to the number of columns.
    decode(repeat('01', column_counts.num_columns::int), 'hex') as replication_mask
from etl.table_mappings tm
join etl.table_schemas ts on ts.pipeline_id = tm.pipeline_id
    and ts.table_id = tm.source_table_id
    and ts.snapshot_id = '0/0'::pg_lsn
join (
    select table_schema_id, count(*) as num_columns
    from etl.table_columns
    group by table_schema_id
) column_counts on column_counts.table_schema_id = ts.id;

-- Drop the old table_mappings table as it is now unified into destination_tables_metadata.
-- This is a breaking change. We intentionally require all pipelines on this branch to upgrade.
drop table etl.table_mappings;
