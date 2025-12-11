-- Add destination_schema_states table for tracking the current schema snapshot_id
-- at each destination and whether a schema change is in progress.
--
-- This is a lightweight tracking table - the actual schema data lives in table_schemas.
-- We can always derive the previous snapshot_id by querying table_schemas if needed for recovery.

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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Only one state per pipeline/table combination.
    UNIQUE (pipeline_id, table_id)
);

-- Index for efficient lookups by pipeline and table.
CREATE INDEX idx_destination_schema_states_pipeline_table
    ON etl.destination_schema_states (pipeline_id, table_id);
