-- Durable destination write stream state.
--
-- Append-only BigQuery writes use explicitly-created committed streams and
-- offsets. Persisting the stream name and next offset lets retries reuse the
-- same stream position after an unknown append result.

create table etl.destination_write_streams (
    id bigint generated always as identity primary key,
    pipeline_id bigint not null,
    table_id oid not null,
    destination_table_id text not null,
    stream_name text not null,
    next_offset bigint not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint destination_write_streams_next_offset_check check (next_offset >= 0),
    unique (pipeline_id, table_id, destination_table_id)
);

create index idx_destination_write_streams_pipeline_table
    on etl.destination_write_streams (pipeline_id, table_id);
