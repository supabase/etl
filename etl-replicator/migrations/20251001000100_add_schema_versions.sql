-- Add schema_version to table schemas and support versioned definitions
alter table etl.table_schemas
    add column if not exists schema_version bigint not null default 0;

-- Adjust unique constraint to account for schema versions
alter table etl.table_schemas
    drop constraint if exists table_schemas_pipeline_id_table_id_key;

alter table etl.table_schemas
    add constraint table_schemas_pipeline_id_table_id_schema_version_key
    unique (pipeline_id, table_id, schema_version);

-- Refresh supporting indexes
drop index if exists idx_table_schemas_pipeline_table;

create index if not exists idx_table_schemas_pipeline_table_version
    on etl.table_schemas (pipeline_id, table_id, schema_version);

-- Remove default now that legacy rows have been backfilled
alter table etl.table_schemas
    alter column schema_version drop default;
