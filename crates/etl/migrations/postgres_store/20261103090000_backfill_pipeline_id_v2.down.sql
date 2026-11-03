-- Keep populated identities and restore the nullable columns for legacy writers.
drop index etl.uq_replication_progress_pipeline_table_sync_v2;
drop index etl.uq_replication_progress_pipeline_apply_v2;
drop index etl.uq_destination_tables_metadata_pipeline_table_v2;
drop index etl.uq_table_schemas_pipeline_table_snapshot_v2;
drop index etl.uq_replication_state_current_true_v2;

alter table etl.replication_state
    drop constraint replication_state_pipeline_id_v2_matches_legacy,
    alter column pipeline_id_v2 drop not null;
alter table etl.table_schemas
    drop constraint table_schemas_pipeline_id_v2_matches_legacy,
    alter column pipeline_id_v2 drop not null;
alter table etl.destination_tables_metadata
    drop constraint destination_tables_metadata_pipeline_id_v2_matches_legacy,
    alter column pipeline_id_v2 drop not null;
alter table etl.replication_progress
    drop constraint replication_progress_pipeline_id_v2_matches_legacy,
    alter column pipeline_id_v2 drop not null;
