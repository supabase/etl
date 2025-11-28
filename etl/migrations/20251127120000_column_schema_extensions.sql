-- Add new column fields to support extended schema information.
--
-- These columns store additional metadata about each column:
-- - primary_key_ordinal_position: The order within the primary key (1-based), NULL if not a primary key
-- - replicated: Whether the column is currently being replicated (default true)

-- Add new columns
alter table etl.table_columns
    add column if not exists primary_key_ordinal_position integer,
    add column if not exists replicated boolean not null default true;

-- Set primary_key_ordinal_position = 1 for existing primary key columns.
-- This is a reasonable default since most tables have single-column primary keys.
update etl.table_columns
set primary_key_ordinal_position = 1
where primary_key = true and primary_key_ordinal_position is null;
