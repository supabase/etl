-- Existing Applying rows may have a previous snapshot, but the corresponding
-- replication mask was never stored and cannot be reconstructed. Leave the
-- new column null for those rows; they require a table resynchronization.
alter table etl.destination_tables_metadata
    add column if not exists previous_replication_mask bytea;
