-- Record whether each stored source schema version has a DEFERRABLE primary
-- key, as reported by `etl.describe_table_identity`.
--
-- Existing rows default to false. The store need not share a database with
-- the source, so their constraint state cannot be read here. The next schema
-- snapshot of a table, or a resynchronization of it, stores the real value.
-- Adding a column with a constant default changes only the catalog and does
-- not rewrite the table.
alter table etl.table_schemas
    add column if not exists primary_key_deferrable boolean not null default false;
