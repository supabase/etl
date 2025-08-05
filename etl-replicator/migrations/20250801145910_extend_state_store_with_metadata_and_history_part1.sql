-- Part 1: Add schema changes and enum value
-- This must be in a separate transaction from using the new enum value

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
create index idx_replication_state_is_current 
    on etl.replication_state (pipeline_id, table_id, is_current);

create index idx_replication_state_prev 
    on etl.replication_state (prev);

-- Create unique index to enforce uniqueness constraint for current states
create unique index uq_replication_state_current_true
    on etl.replication_state (pipeline_id, table_id)
    where is_current = true;

-- Update the enum to include 'errored' 
alter type etl.table_state add value 'errored';