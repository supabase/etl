create type etl.table_state as enum (
    'init',
    'data_sync',
    'finished_copy',
    'sync_done',
    'ready',
    'skipped'
);

create table
    etl.replication_state (
        pipeline_id bigint not null,
        table_id oid not null,
        state table_state not null,
        sync_done_lsn pg_lsn null,
        primary key (pipeline_id, table_id)
    );