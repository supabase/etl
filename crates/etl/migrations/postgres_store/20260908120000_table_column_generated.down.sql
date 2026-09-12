-- Drops the stored generated-column flag.
--
-- Stored schemas become indistinguishable from non-generated ones, so the
-- replication-mask fallbacks would treat generated columns as replicated and
-- fail row decoding. Before running this down, revert the matching source
-- migration so `etl.describe_table_schema` stops returning generated columns,
-- and reset every publication to `publish_generated_columns = none`.

alter table etl.table_columns
    drop column if exists generated;
