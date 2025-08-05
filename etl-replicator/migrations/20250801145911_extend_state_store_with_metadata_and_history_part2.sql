-- Part 2: Migrate data using the new enum value
-- This must be in a separate transaction after the enum value is committed

-- Migrate existing SyncDone state to flattened JSONB metadata
update etl.replication_state
set metadata = jsonb_build_object('type', 'sync_done', 'lsn', sync_done_lsn)
where state = 'sync_done' and sync_done_lsn is not null;

-- For SyncDone states without LSN, set a default
update etl.replication_state
set metadata = jsonb_build_object('type', 'sync_done', 'lsn', '0/0')
where state = 'sync_done' and sync_done_lsn is null;

-- Migrate 'skipped' states to 'errored' with appropriate metadata
update etl.replication_state
set 
    state = 'errored',
    metadata = jsonb_build_object(
        'type', 'errored',
        'reason', 'An error occurred in ETL',
        'retry_policy', jsonb_build_object('type', 'no_retry')
    )
where state = 'skipped';

-- Migrate other states to have type-only metadata
update etl.replication_state
set metadata = jsonb_build_object('type', 
    case state
        when 'init' then 'init'
        when 'data_sync' then 'data_sync'
        when 'finished_copy' then 'finished_copy'
        when 'ready' then 'ready'
        when 'errored' then 'errored'
    end
)
where metadata is null;

-- Add the new primary key after adding id column
alter table etl.replication_state drop constraint replication_state_pkey;
alter table etl.replication_state add primary key (id);

-- Add foreign key constraint for prev column
alter table etl.replication_state 
    add constraint fk_replication_state_prev 
    foreign key (prev) references etl.replication_state(id);

-- Drop the deprecated sync_done_lsn column since LSN is now stored in metadata
alter table etl.replication_state drop column sync_done_lsn;