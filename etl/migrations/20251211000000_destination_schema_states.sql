-- Add destination_schema_states table for tracking the current schema snapshot_id
-- at each destination and whether a schema change is in progress.
--
-- This table stores the replication mask alongside the snapshot_id so that when
-- schema changes occur, we can reconstruct the old ReplicatedTableSchema with
-- the correct mask for accurate diffing.

CREATE TABLE etl.destination_schema_states (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id BIGINT NOT NULL,
    table_id OID NOT NULL,
    -- State type: 'applying' when a schema change is in progress, 'applied' when complete.
    -- If 'applying' is found on startup, the system knows the destination schema may be
    -- in an unknown state and recovery is needed.
    state_type TEXT NOT NULL CHECK (state_type IN ('applying', 'applied')),
    -- The current snapshot_id at the destination.
    snapshot_id BIGINT NOT NULL,
    -- The replication mask as a byte array where each byte is 0 (not replicated) or 1 (replicated).
    -- The index corresponds to the column's ordinal position in the schema.
    replication_mask BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Only one state per pipeline/table combination.
    UNIQUE (pipeline_id, table_id)
);

-- Index for efficient lookups by pipeline and table.
CREATE INDEX idx_destination_schema_states_pipeline_table
    ON etl.destination_schema_states (pipeline_id, table_id);
