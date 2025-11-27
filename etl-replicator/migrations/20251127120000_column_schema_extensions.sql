-- Add new column fields to support extended schema information.
--
-- These columns store additional metadata about each column:
-- - data_type: The formatted Postgres type string (e.g., 'character varying(255)')
-- - primary_key_position: The position within the primary key (1-based), NULL if not a primary key
-- - replicated: Whether the column is currently being replicated (default true)
--
-- Also renames column_order to ordinal_position for consistency and removes the
-- redundant primary_key boolean column (replaced by primary_key_position).

-- Add new columns
alter table etl.table_columns
    add column if not exists data_type text,
    add column if not exists primary_key_position integer,
    add column if not exists replicated boolean not null default true;

-- Update existing rows to have sensible defaults for data_type based on column_type.
-- The column_type stores the internal Postgres type name (e.g., 'TEXT', 'INT4'),
-- while data_type should store the formatted type string. For existing rows,
-- we'll use the column_type as a fallback.
update etl.table_columns
set data_type = column_type
where data_type is null;

-- Set primary_key_position = 1 for existing primary key columns.
-- This is a reasonable default since most tables have single-column primary keys.
update etl.table_columns
set primary_key_position = 1
where primary_key = true and primary_key_position is null;

-- Rename column_order to ordinal_position for consistency with the describe_table_schema function.
alter table etl.table_columns
    rename column column_order to ordinal_position;

-- Drop the redundant primary_key boolean column (replaced by primary_key_position).
-- We need to drop the unique constraint first.
alter table etl.table_columns
    drop constraint if exists table_columns_table_schema_id_column_order_key;

alter table etl.table_columns
    drop column if exists primary_key;

-- Add back unique constraint with new column name.
alter table etl.table_columns
    add constraint table_columns_table_schema_id_ordinal_position_key
    unique (table_schema_id, ordinal_position);
