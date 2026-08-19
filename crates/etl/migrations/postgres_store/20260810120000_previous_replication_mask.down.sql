alter table etl.destination_tables_metadata
    drop column if exists previous_replication_mask;
