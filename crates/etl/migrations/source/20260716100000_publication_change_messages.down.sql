drop event trigger if exists supabase_etl_publication_change_drop_trigger;
drop event trigger if exists supabase_etl_publication_change_ddl_trigger;

drop function if exists etl.emit_publication_drop_change_message();
drop function if exists etl.emit_publication_ddl_change_message();
drop function if exists etl.emit_publication_change_message(pg_catalog.text);
