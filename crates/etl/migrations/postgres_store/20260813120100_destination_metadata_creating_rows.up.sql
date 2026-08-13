-- Only legacy initial creation has no previous endpoint. An Applying row with
-- a previous snapshot but no previous replication mask is an unrecoverable
-- schema change from before that mask was stored and must remain Applying.
update etl.destination_tables_metadata
set schema_status = 'creating'
where schema_status = 'applying'
  and previous_snapshot_id is null
  and previous_replication_mask is null;
