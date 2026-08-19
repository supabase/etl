alter type etl.destination_table_schema_status
    add value if not exists 'creating' before 'applying';
