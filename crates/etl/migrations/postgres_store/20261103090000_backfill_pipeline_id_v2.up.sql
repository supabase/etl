-- Deploy after all writers support pipeline_id_v2. Backfill and
-- constraints commit together; a conflicting existing identity aborts the
-- migration rather than replacing it. Numeric keys remain authoritative.
-- Acquire the final DDL locks before touching rows, avoiding a later upgrade
-- from update locks while another pipeline is reading the same state tables.
lock table etl.replication_state, etl.table_schemas,
    etl.destination_tables_metadata, etl.replication_progress in access exclusive mode;

update etl.replication_state
set pipeline_id_v2 = pipeline_id::text
where pipeline_id_v2 is null;
update etl.table_schemas
set pipeline_id_v2 = pipeline_id::text
where pipeline_id_v2 is null;
update etl.destination_tables_metadata
set pipeline_id_v2 = pipeline_id::text
where pipeline_id_v2 is null;
update etl.replication_progress
set pipeline_id_v2 = pipeline_id::text
where pipeline_id_v2 is null;

alter table etl.replication_state
    alter column pipeline_id_v2 set not null,
    add constraint replication_state_pipeline_id_v2_matches_legacy
        check (pipeline_id >= 0 and pipeline_id_v2 = pipeline_id::text);
alter table etl.table_schemas
    alter column pipeline_id_v2 set not null,
    add constraint table_schemas_pipeline_id_v2_matches_legacy
        check (pipeline_id >= 0 and pipeline_id_v2 = pipeline_id::text);
alter table etl.destination_tables_metadata
    alter column pipeline_id_v2 set not null,
    add constraint destination_tables_metadata_pipeline_id_v2_matches_legacy
        check (pipeline_id >= 0 and pipeline_id_v2 = pipeline_id::text);
alter table etl.replication_progress
    alter column pipeline_id_v2 set not null,
    add constraint replication_progress_pipeline_id_v2_matches_legacy
        check (pipeline_id >= 0 and pipeline_id_v2 = pipeline_id::text);

create unique index uq_replication_state_current_true_v2
    on etl.replication_state (pipeline_id_v2, table_id) where is_current = true;
create unique index uq_table_schemas_pipeline_table_snapshot_v2
    on etl.table_schemas (pipeline_id_v2, table_id, snapshot_id);
create unique index uq_destination_tables_metadata_pipeline_table_v2
    on etl.destination_tables_metadata (pipeline_id_v2, table_id);
create unique index uq_replication_progress_pipeline_apply_v2
    on etl.replication_progress (pipeline_id_v2, worker_type) where table_id is null;
create unique index uq_replication_progress_pipeline_table_sync_v2
    on etl.replication_progress (pipeline_id_v2, worker_type, table_id) where table_id is not null;
