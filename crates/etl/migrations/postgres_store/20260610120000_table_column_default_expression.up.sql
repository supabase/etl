alter table etl.table_columns
    add column if not exists default_expression text;
