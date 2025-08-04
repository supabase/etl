-- Add the id column first as a regular BIGINT
alter table etl.replication_state add column id bigint;

-- Create a sequence for the ID column
create sequence etl.replication_state_id_seq;

-- Backfill existing rows with sequential IDs
update etl.replication_state 
set id = nextval('etl.replication_state_id_seq');

-- Set the id column to NOT NULL and make it use the sequence as default
alter table etl.replication_state 
    alter column id set not null,
    alter column id set default nextval('etl.replication_state_id_seq');

-- Set the sequence ownership to the column (makes it behave like BIGSERIAL)
alter sequence etl.replication_state_id_seq owned by etl.replication_state.id;

-- Add the other new columns
alter table etl.replication_state
    add column metadata jsonb,
    add column prev bigint,
    add column is_current boolean not null default true;

-- Create indexes for performance
create index concurrently idx_replication_state_is_current 
    on etl.replication_state (pipeline_id, table_id, is_current);

create index concurrently idx_replication_state_prev 
    on etl.replication_state (prev);

-- Create unique index to enforce uniqueness constraint for current states
create unique index uq_replication_state_current_true
    on etl.replication_state (pipeline_id, table_id)
    where is_current = true;

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
        'reason', 'Migrated error',
        'solution', 'Migrated solution',
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

-- Update the enum to include 'errored' and remove 'skipped'
alter type etl.table_state add value 'errored';

-- Drop the deprecated sync_done_lsn column since LSN is now stored in metadata
alter table etl.replication_state drop column sync_done_lsn;

-- Note: We cannot remove 'skipped' from enum in same transaction.