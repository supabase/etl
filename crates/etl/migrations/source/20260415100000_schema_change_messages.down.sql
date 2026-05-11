drop event trigger if exists supabase_etl_ddl_message_trigger;

drop function if exists etl.emit_schema_change_messages();
drop function if exists etl.describe_table_identity(pg_catalog.oid);
drop function if exists etl.describe_table_schema(pg_catalog.oid);
