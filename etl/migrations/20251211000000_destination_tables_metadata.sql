-- Unified destination table metadata.
--
-- Tracks all destination-related state for each replicated table in a single row.

-- Enum for destination table schema status.
CREATE TYPE etl.destination_table_schema_status AS ENUM (
    'applying',
    'applied'
);

CREATE TABLE etl.destination_tables_metadata (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id BIGINT NOT NULL,
    table_id OID NOT NULL,
    -- The name/identifier of the table in the destination system.
    destination_table_id TEXT NOT NULL,
    -- The snapshot_id of the schema currently applied at the destination.
    snapshot_id PG_LSN NOT NULL,
    -- Status: 'applying' when a schema change is in progress, 'applied' when complete.
    -- If 'applying' is found on startup, recovery may be needed.
    schema_status etl.destination_table_schema_status NOT NULL,
    -- The replication mask as a byte array where each byte is 0 (not replicated) or 1 (replicated).
    -- The index corresponds to the column's ordinal position in the schema.
    replication_mask BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- One metadata row per table per pipeline.
    UNIQUE (pipeline_id, table_id)
);

-- Backfill destination_tables_metadata from existing table_mappings and table_schemas.
-- For each mapped table, create metadata with snapshot_id = 0/0 (initial schema)
-- and all columns marked as replicated.
INSERT INTO etl.destination_tables_metadata (
    pipeline_id,
    table_id,
    destination_table_id,
    snapshot_id,
    schema_status,
    replication_mask
)
SELECT
    tm.pipeline_id,
    tm.source_table_id AS table_id,
    tm.destination_table_id,
    '0/0'::pg_lsn AS snapshot_id,
    'applied'::etl.destination_table_schema_status,
    -- Create a bytea of 1s with length equal to the number of columns.
    decode(repeat('01', column_counts.num_columns::int), 'hex') AS replication_mask
FROM etl.table_mappings tm
JOIN etl.table_schemas ts ON ts.pipeline_id = tm.pipeline_id
    AND ts.table_id = tm.source_table_id
    AND ts.snapshot_id = '0/0'::pg_lsn
JOIN (
    SELECT table_schema_id, COUNT(*) AS num_columns
    FROM etl.table_columns
    GROUP BY table_schema_id
) column_counts ON column_counts.table_schema_id = ts.id;

-- Drop the old table_mappings table as it's now unified into destination_tables_metadata.
-- This is a breaking change, for now we assume that no two pipelines share the same state storage.
DROP TABLE etl.table_mappings;
