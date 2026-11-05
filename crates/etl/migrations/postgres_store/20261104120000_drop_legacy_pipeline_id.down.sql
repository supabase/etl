-- Restore the nullable compatibility columns and indexes for the preceding
-- string-key release. Discarded legacy values are not reconstructed; all
-- identities and progress remain in V2.
lock table etl.replication_state, etl.table_schemas,
    etl.destination_tables_metadata, etl.replication_progress in access exclusive mode;

alter table etl.replication_state add column pipeline_id bigint;
alter table etl.table_schemas add column pipeline_id bigint;
alter table etl.destination_tables_metadata add column pipeline_id bigint;
alter table etl.replication_progress add column pipeline_id bigint;

create unique index uq_replication_state_current_true
    on etl.replication_state (pipeline_id, table_id) where is_current = true;
alter table etl.table_schemas
    add constraint table_schemas_pipeline_id_table_id_snapshot_id_key
        unique (pipeline_id, table_id, snapshot_id);
create index idx_table_schemas_pipeline_table
    on etl.table_schemas (pipeline_id, table_id);
create index idx_table_schemas_pipeline_table_snapshot_id
    on etl.table_schemas (pipeline_id, table_id, snapshot_id desc);
alter table etl.destination_tables_metadata
    add constraint destination_tables_metadata_pipeline_id_table_id_key
        unique (pipeline_id, table_id);
create unique index uq_replication_progress_pipeline_apply
    on etl.replication_progress (pipeline_id, worker_type) where table_id is null;
create unique index uq_replication_progress_pipeline_table_sync
    on etl.replication_progress (pipeline_id, worker_type, table_id) where table_id is not null;
