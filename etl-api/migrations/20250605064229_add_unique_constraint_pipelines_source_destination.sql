-- Add unique constraint to ensure only one pipeline can exist for a given
-- tenant_id, source_id, and destination_id combination
ALTER TABLE app.pipelines
ADD CONSTRAINT pipelines_tenant_source_destination_unique 
UNIQUE (tenant_id, source_id, destination_id);
