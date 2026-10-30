alter table etl.external_maintenance_state
    alter column pipeline_id type text using pipeline_id::text;
