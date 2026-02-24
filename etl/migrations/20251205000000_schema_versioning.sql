-- Add snapshot_id column to table_schemas for schema versioning.
-- The snapshot_id value is the start_lsn of the DDL message that created this schema version.
-- Initial schemas use snapshot_id='0/0'.

ALTER TABLE etl.table_schemas
    ADD COLUMN IF NOT EXISTS snapshot_id PG_LSN NOT NULL DEFAULT '0/0';

-- Change unique constraint from (pipeline_id, table_id) to (pipeline_id, table_id, snapshot_id)
-- to allow multiple schema versions per table.
ALTER TABLE etl.table_schemas
    DROP CONSTRAINT IF EXISTS table_schemas_pipeline_id_table_id_key;

ALTER TABLE etl.table_schemas
    ADD CONSTRAINT table_schemas_pipeline_id_table_id_snapshot_id_key
    UNIQUE (pipeline_id, table_id, snapshot_id);

-- Index for efficient "find largest snapshot_id <= X" queries.
CREATE INDEX IF NOT EXISTS idx_table_schemas_pipeline_table_snapshot_id
    ON etl.table_schemas (pipeline_id, table_id, snapshot_id DESC);
