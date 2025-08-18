-- Add timestamp columns to the replication_state table for auditing
-- This follows the same pattern as other ETL tables (table_schemas, table_mappings)

alter table etl.replication_state
    add column created_at timestamptz not null default now(),
    add column updated_at timestamptz not null default now();