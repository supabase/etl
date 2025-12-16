-- Add new column fields to support extended schema information.
--
-- These columns store additional metadata about each column:
-- - primary_key_ordinal_position: The order within the primary key (1-based), NULL if not a primary key

-- Add new columns
alter table etl.table_columns
    add column if not exists primary_key_ordinal_position pg_catalog.int4;

-- Backfill primary_key_ordinal_position by querying the actual PK constraint from pg_constraint.
-- This assumes the source tables still exist and their PK order hasn't changed.
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
