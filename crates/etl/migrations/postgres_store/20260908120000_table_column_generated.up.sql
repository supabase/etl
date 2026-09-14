-- Records whether a stored table column is a PostgreSQL stored generated column.
--
-- Existing rows are correctly false: they were snapshotted by a
-- `etl.describe_table_schema` version that excluded generated columns entirely,
-- so no stored row can describe one. The constant default avoids a table
-- rewrite and keeps the column readable by the previous release, which simply
-- ignores it.

alter table etl.table_columns
    add column if not exists generated boolean not null default false;
