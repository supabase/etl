CREATE TABLE IF NOT EXISTS public.etl_external_maintenance_state (
    pipeline_id BIGINT PRIMARY KEY,
    active_run JSONB,
    pause_request JSONB,
    operation_request JSONB,
    replicator JSONB,
    last_successful_operations JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_completed_at TIMESTAMPTZ,
    operation_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
