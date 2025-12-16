-- Add new column fields to support extended schema information.
--
-- These columns store additional metadata about each column:
-- - primary_key_ordinal_position: The order within the primary key (1-based), NULL if not a primary key

-- Add new columns
alter table etl.table_columns
    add column if not exists primary_key_ordinal_position pg_catalog.int4;

-- Set primary_key_ordinal_position for existing primary key columns.
-- For composite PKs, we assign ordinal positions based on the column_order within each table,
-- which is a reasonable approximation when the original PK constraint order is unavailable.
update etl.table_columns tc
set primary_key_ordinal_position = pk_order.ordinal
from (
    select
        id,
        row_number() over (partition by table_schema_id order by column_order) as ordinal
    from etl.table_columns
    where primary_key = true
) pk_order
where tc.id = pk_order.id
  and tc.primary_key_ordinal_position is null;
