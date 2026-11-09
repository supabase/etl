-- Deploy after every reader uses V2 and every writer has stopped populating
-- legacy keys. Dropping the columns also removes their dependent indexes and
-- constraints; V2 keys and indexes remain unchanged.
lock table etl.replication_state, etl.table_schemas,
    etl.destination_tables_metadata, etl.replication_progress in access exclusive mode;

alter table etl.replication_state drop column pipeline_id;
alter table etl.table_schemas drop column pipeline_id;
alter table etl.destination_tables_metadata drop column pipeline_id;
alter table etl.replication_progress drop column pipeline_id;
