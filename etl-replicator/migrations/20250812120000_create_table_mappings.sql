-- Create table mappings storage system in the state store
-- This stores mappings between source table IDs (postgres OIDs) and destination table IDs

-- Table to store table ID mappings from source to destination
create table etl.table_mappings (
    id bigserial primary key,
    pipeline_id bigint not null,
    source_table_id oid not null,
    destination_table_id text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    
    -- Ensure unique combination per pipeline and source table
    unique (pipeline_id, source_table_id)
);

-- Index to speed up queries by pipeline_id for loading all mappings
create index idx_table_mappings_pipeline 
    on etl.table_mappings (pipeline_id);

-- Index to speed up queries by pipeline_id and source_table_id for single mapping lookups
create index idx_table_mappings_pipeline_source 
    on etl.table_mappings (pipeline_id, source_table_id);