-- Deploy after API readers use pipeline_id_v2. String keys become authoritative;
-- nullable legacy keys remain for older pipeline writers during fleet rollout.
lock table etl.replication_state, etl.table_schemas,
    etl.destination_tables_metadata, etl.replication_progress in access exclusive mode;

alter table etl.replication_state
    drop constraint replication_state_pipeline_id_v2_matches_legacy,
    alter column pipeline_id drop not null;
alter table etl.table_schemas
    drop constraint table_schemas_pipeline_id_v2_matches_legacy,
    alter column pipeline_id drop not null;
alter table etl.destination_tables_metadata
    drop constraint destination_tables_metadata_pipeline_id_v2_matches_legacy,
    alter column pipeline_id drop not null;
alter table etl.replication_progress
    drop constraint replication_progress_pipeline_id_v2_matches_legacy,
    alter column pipeline_id drop not null;
