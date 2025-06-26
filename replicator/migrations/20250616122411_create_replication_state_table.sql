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
        -- TODO: should use the pg_lsn type here but simplifying because sqlx doen't yet support a PgLsn type
        -- and we'd have to implement it manually (and the Encode/Decode) traits to make it work. So taking
        -- the simpler approach of using text type for now.
        sync_done_lsn text null,
        primary key (pipeline_id, table_id)
    );