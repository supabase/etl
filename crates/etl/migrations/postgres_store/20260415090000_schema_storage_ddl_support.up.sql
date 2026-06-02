-- Storage changes for DDL-aware schema tracking.
--
-- This is an intentional one-way upgrade for this branch:
-- - `etl.table_columns.column_order` is renamed to `ordinal_position`.
-- - `etl.table_columns.primary_key` is replaced by `primary_key_ordinal_position`.
-- - existing stored values are shifted by `+ 1` to match PostgreSQL's 1-based ordinals.
-- - no backward-compatibility layer is kept for older pipelines.
--
-- All pipelines must be upgraded together with code that reads `ordinal_position`.

-- Rename the stored column order field to its new steady-state name.
alter table etl.table_columns
    rename column column_order to ordinal_position;

-- Shift stored ordinals from 0-based to 1-based without violating the
-- existing unique constraint on (table_schema_id, ordinal_position).
update etl.table_columns
set ordinal_position = ordinal_position + 1000000;
update etl.table_columns
set ordinal_position = ordinal_position - 999999;

-- Add the new primary-key position field.
alter table etl.table_columns
    add column if not exists primary_key_ordinal_position integer;

-- Backfill primary_key_ordinal_position from the live primary-key definition
-- for each stored schema row.
update etl.table_columns
set primary_key_ordinal_position = null
where primary_key = false;

with live_primary_key_columns as (
    select
        ts.id as table_schema_id,
        a.attname as column_name,
        x.n::pg_catalog.int4 as pk_position
    from etl.table_schemas ts
    join pg_catalog.pg_constraint con
      on con.conrelid = ts.table_id
     and con.contype = 'p'
    cross join lateral unnest(con.conkey) with ordinality as x(attnum, n)
    join pg_catalog.pg_attribute a
      on a.attrelid = ts.table_id
     and a.attnum = x.attnum
)
update etl.table_columns tc
set primary_key_ordinal_position = lpkc.pk_position
from live_primary_key_columns lpkc
where tc.table_schema_id = lpkc.table_schema_id
  and tc.column_name = lpkc.column_name
  and tc.primary_key = true;

-- Drop the old boolean primary-key flag now that the ordinal form is
-- populated.
alter table etl.table_columns
    drop column primary_key;

-- Add snapshot_id so one table can have multiple stored schema versions over
-- time. Existing rows become the initial version at `0/0`.
alter table etl.table_schemas
    add column if not exists snapshot_id pg_lsn not null default '0/0';

-- Expand the uniqueness key to include snapshot_id.
alter table etl.table_schemas
    drop constraint if exists table_schemas_pipeline_id_table_id_key;
alter table etl.table_schemas
    add constraint table_schemas_pipeline_id_table_id_snapshot_id_key
    unique (pipeline_id, table_id, snapshot_id);

-- Add the lookup index used to find the latest schema at or before a given
-- snapshot.
create index if not exists idx_table_schemas_pipeline_table_snapshot_id
    on etl.table_schemas (pipeline_id, table_id, snapshot_id desc);

-- Add the status enum used by destination schema application.
create type etl.destination_table_schema_status as enum (
    'applying',
    'applied'
);

-- Create the new destination metadata table that replaces the old
-- `table_mappings` rows with a richer per-table state record.
create table etl.destination_tables_metadata (
    id bigint generated always as identity primary key,
    pipeline_id bigint not null,
    table_id oid not null,
    destination_table_id text not null,
    snapshot_id pg_lsn not null,
    previous_snapshot_id pg_lsn,
    schema_status etl.destination_table_schema_status not null,
    replication_mask bytea not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (pipeline_id, table_id)
);

-- Backfill the new destination metadata table from the old mappings. Existing
-- rows become `snapshot_id = 0/0`, `schema_status = applied`, and an all-ones
-- replication mask, while preserving the original timestamps.
insert into etl.destination_tables_metadata (
    pipeline_id,
    table_id,
    destination_table_id,
    snapshot_id,
    schema_status,
    replication_mask,
    created_at,
    updated_at
)
with initial_schema_matches as (
    select
        tm.pipeline_id,
        tm.source_table_id,
        tm.destination_table_id,
        min(ts.id) as table_schema_id
    from etl.table_mappings tm
    left join etl.table_schemas ts
      on ts.pipeline_id = tm.pipeline_id
     and ts.table_id = tm.source_table_id
     and ts.snapshot_id = '0/0'::pg_catalog.pg_lsn
    group by tm.pipeline_id, tm.source_table_id, tm.destination_table_id
),
schema_stats as (
    select
        ts.id as table_schema_id,
        count(tc.id)::pg_catalog.int4 as column_count
    from etl.table_schemas ts
    left join etl.table_columns tc
      on tc.table_schema_id = ts.id
    group by ts.id
),
mapping_backfill as (
    select
        tm.pipeline_id,
        tm.source_table_id as table_id,
        tm.destination_table_id,
        tm.created_at,
        tm.updated_at,
        ss.column_count
    from etl.table_mappings tm
    join initial_schema_matches ism
      on ism.pipeline_id = tm.pipeline_id
     and ism.source_table_id = tm.source_table_id
     and ism.destination_table_id = tm.destination_table_id
    join schema_stats ss
      on ss.table_schema_id = ism.table_schema_id
)
select
    mb.pipeline_id,
    mb.table_id,
    mb.destination_table_id,
    '0/0'::pg_catalog.pg_lsn as snapshot_id,
    'applied'::etl.destination_table_schema_status,
    -- Create one `0x01` byte per stored column in ordinal order.
    decode(repeat('01', mb.column_count::int), 'hex') as replication_mask,
    mb.created_at,
    mb.updated_at
from mapping_backfill mb;

-- Remove the old mapping table now that its state lives in
-- `destination_tables_metadata`.
drop table etl.table_mappings;
