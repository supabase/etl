-- Downgrade readers before dropping their captured missing-value metadata.
-- Legacy readers continue using default_expression and cannot retain the
-- separate add-time-value behavior introduced by this migration.
alter table etl.table_columns
    drop column missing_value_expression;
