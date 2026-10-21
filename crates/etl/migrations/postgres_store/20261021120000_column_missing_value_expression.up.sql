-- Retain add-time values independently of defaults for future inserts. NULL
-- leaves existing snapshots on their legacy default-expression fallback;
-- historical missing values cannot be reconstructed from current catalogs.
-- Older binaries omit this nullable column in their explicit insert/select lists.
alter table etl.table_columns
    add column missing_value_expression text;
