-- Rollback requires canonical numeric identities. Casts and matching constraints
-- abort the transaction rather than discarding or remapping string identities.
lock table etl.replication_state, etl.table_schemas,
    etl.destination_tables_metadata, etl.replication_progress in access exclusive mode;

update etl.replication_state set pipeline_id = pipeline_id_v2::bigint where pipeline_id is null;
update etl.table_schemas set pipeline_id = pipeline_id_v2::bigint where pipeline_id is null;
update etl.destination_tables_metadata set pipeline_id = pipeline_id_v2::bigint where pipeline_id is null;
update etl.replication_progress set pipeline_id = pipeline_id_v2::bigint where pipeline_id is null;

alter table etl.replication_state
    alter column pipeline_id set not null,
    add constraint replication_state_pipeline_id_v2_matches_legacy
        check (pipeline_id >= 0 and pipeline_id_v2 = pipeline_id::text);
alter table etl.table_schemas
    alter column pipeline_id set not null,
    add constraint table_schemas_pipeline_id_v2_matches_legacy
        check (pipeline_id >= 0 and pipeline_id_v2 = pipeline_id::text);
alter table etl.destination_tables_metadata
    alter column pipeline_id set not null,
    add constraint destination_tables_metadata_pipeline_id_v2_matches_legacy
        check (pipeline_id >= 0 and pipeline_id_v2 = pipeline_id::text);
alter table etl.replication_progress
    alter column pipeline_id set not null,
    add constraint replication_progress_pipeline_id_v2_matches_legacy
        check (pipeline_id >= 0 and pipeline_id_v2 = pipeline_id::text);
