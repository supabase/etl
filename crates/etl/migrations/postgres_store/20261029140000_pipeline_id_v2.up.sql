alter table etl.replication_state add column pipeline_id_v2 text;
alter table etl.table_schemas add column pipeline_id_v2 text;
alter table etl.destination_tables_metadata add column pipeline_id_v2 text;
alter table etl.replication_progress add column pipeline_id_v2 text;
