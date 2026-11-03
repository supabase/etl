alter table etl.external_maintenance_state
    alter column pipeline_id type bigint using pipeline_id::bigint;
