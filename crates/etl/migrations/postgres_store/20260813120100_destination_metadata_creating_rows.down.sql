update etl.destination_tables_metadata
set schema_status = 'applying'
where schema_status = 'creating';
