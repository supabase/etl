alter table etl.replication_progress drop column pipeline_id_v2;
alter table etl.destination_tables_metadata drop column pipeline_id_v2;
alter table etl.table_schemas drop column pipeline_id_v2;
alter table etl.replication_state drop column pipeline_id_v2;
