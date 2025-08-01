-- Add new columns to support metadata, history chaining, and current state tracking
ALTER TABLE etl.replication_state
    ADD COLUMN id BIGSERIAL,
    ADD COLUMN metadata JSONB,
    ADD COLUMN prev BIGINT,
    ADD COLUMN is_current BOOLEAN NOT NULL DEFAULT true;

-- Create indexes for performance
CREATE INDEX CONCURRENTLY idx_replication_state_is_current 
    ON etl.replication_state (pipeline_id, table_id, is_current);

CREATE INDEX CONCURRENTLY idx_replication_state_prev 
    ON etl.replication_state (prev);

-- Migrate existing SyncDone state to flattened JSONB metadata
UPDATE etl.replication_state
SET metadata = jsonb_build_object('type', 'sync_done', 'lsn', sync_done_lsn)
WHERE state = 'sync_done' AND sync_done_lsn IS NOT NULL;

-- For SyncDone states without LSN, set a default
UPDATE etl.replication_state
SET metadata = jsonb_build_object('type', 'sync_done', 'lsn', '0/0')
WHERE state = 'sync_done' AND sync_done_lsn IS NULL;

-- Migrate 'skipped' states to 'errored' with appropriate metadata
UPDATE etl.replication_state
SET 
    state = 'errored',
    metadata = jsonb_build_object(
        'type', 'errored',
        'reason', 'Migrated error',
        'solution', 'Migrated solution',
        'retry_policy', jsonb_build_object('type', 'none')
    )
WHERE state = 'skipped';

-- Migrate other states to have type-only metadata
UPDATE etl.replication_state
SET metadata = jsonb_build_object('type', 
    CASE state
        WHEN 'init' THEN 'init'
        WHEN 'data_sync' THEN 'data_sync'
        WHEN 'finished_copy' THEN 'finished_copy'
        WHEN 'ready' THEN 'ready'
    END
)
WHERE metadata IS NULL;

-- Add the new primary key after adding id column
ALTER TABLE etl.replication_state DROP CONSTRAINT replication_state_pkey;
ALTER TABLE etl.replication_state ADD PRIMARY KEY (id);

-- Add foreign key constraint for prev column
ALTER TABLE etl.replication_state 
    ADD CONSTRAINT fk_replication_state_prev 
    FOREIGN KEY (prev) REFERENCES etl.replication_state(id);

-- Add unique constraint to ensure only one current state per (pipeline_id, table_id)
ALTER TABLE etl.replication_state 
    ADD CONSTRAINT uq_replication_state_current 
    UNIQUE (pipeline_id, table_id, is_current) 
    DEFERRABLE INITIALLY DEFERRED;

-- Update the enum to include 'errored' and remove 'skipped'
ALTER TYPE etl.table_state ADD VALUE 'errored';

-- Drop the deprecated sync_done_lsn column since LSN is now stored in metadata
ALTER TABLE etl.replication_state DROP COLUMN sync_done_lsn;

-- Note: We cannot remove 'skipped' from enum in same transaction, 
-- but we'll handle the migration in the Rust code