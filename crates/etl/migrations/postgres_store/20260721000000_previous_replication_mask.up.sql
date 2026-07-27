alter table etl.destination_tables_metadata
    add column if not exists previous_replication_mask bytea;
