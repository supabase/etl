-- Create table schema storage system in the state store
-- This stores table schemas in a normalized structure for efficient access and persistence

-- Table to store table-level schema information
create table etl.table_schemas (
    id bigserial primary key,
    pipeline_id bigint not null,
    table_id oid not null,
    schema_name text not null,
    table_name text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    
    -- Ensure unique combination per pipeline
    unique (pipeline_id, table_id)
);

-- Table to store column-level schema information
create table etl.table_columns (
    id bigserial primary key,
    table_schema_id bigint not null references etl.table_schemas(id) on delete cascade,
    column_name text not null,
    column_type text not null,
    type_modifier integer not null,
    nullable boolean not null,
    primary_key boolean not null,
    column_order integer not null,
    created_at timestamptz not null default now(),
    
    -- Ensure unique column names per table schema and enforce ordering
    unique (table_schema_id, column_name),
    unique (table_schema_id, column_order)
);

-- Indexes for performance
create index idx_table_schemas_pipeline_table 
    on etl.table_schemas (pipeline_id, table_id);

create index idx_table_schemas_pipeline 
    on etl.table_schemas (pipeline_id);

create index idx_table_columns_table_schema 
    on etl.table_columns (table_schema_id);

create index idx_table_columns_order 
    on etl.table_columns (table_schema_id, column_order);

-- Function to update the updated_at timestamp
create or replace function etl.update_table_schemas_updated_at()
returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

-- Trigger to automatically update the updated_at column
create trigger trigger_update_table_schemas_updated_at
    before update on etl.table_schemas
    for each row
    execute function etl.update_table_schemas_updated_at();