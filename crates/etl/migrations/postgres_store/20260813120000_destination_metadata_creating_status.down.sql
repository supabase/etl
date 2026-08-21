alter type etl.destination_table_schema_status
    rename to destination_table_schema_status_with_creating;

create type etl.destination_table_schema_status as enum (
    'applying',
    'applied'
);

alter table etl.destination_tables_metadata
    alter column schema_status type etl.destination_table_schema_status
    using schema_status::text::etl.destination_table_schema_status;

drop type etl.destination_table_schema_status_with_creating;
