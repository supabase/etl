-- Adds helper functions and event trigger to emit schema change messages via logical decoding.
create or replace function etl.describe_table_schema(p_schema text, p_table text)
returns jsonb
language sql
stable
as
$$
select jsonb_agg(
               jsonb_build_object(
                       'column_name', c.column_name,
                       'ordinal_position', c.ordinal_position,
                       'data_type', c.data_type,
                       'is_nullable', c.is_nullable,
                       'column_default', c.column_default,
                       'is_primary_key', (kcu.column_name is not null),
                       'primary_key_position', kcu.ordinal_position
               ) order by c.ordinal_position
       )
from information_schema.columns c
         left join information_schema.key_column_usage kcu
                   on kcu.table_schema = c.table_schema
                       and kcu.table_name = c.table_name
                       and kcu.column_name = c.column_name
                       and kcu.constraint_name in (select constraint_name
                                                   from information_schema.table_constraints
                                                   where table_schema = c.table_schema
                                                     and table_name = c.table_name
                                                     and constraint_type = 'PRIMARY KEY')
where c.table_schema = p_schema
  and c.table_name = p_table;
$$;

create or replace function etl.emit_schema_change_messages()
returns event_trigger
language plpgsql
as
$$
declare
    cmd record;
    table_schema text;
    table_name text;
    table_oid oid;
    schema_json jsonb;
    msg_json jsonb;
begin
    for cmd in
        select * from pg_event_trigger_ddl_commands()
    loop
        if cmd.object_type not in ('table', 'column') then
            continue;
        end if;

        table_oid := cmd.objid;

        if table_oid is null then
            continue;
        end if;

        select n.nspname, c.relname
        into table_schema, table_name
        from pg_class c
                 join pg_namespace n on n.oid = c.relnamespace
        where c.oid = table_oid
          and c.relkind = 'r';

        if table_schema is null or table_name is null then
            continue;
        end if;

        select etl.describe_table_schema(table_schema, table_name)
        into schema_json;

        if schema_json is null then
            continue;
        end if;

        msg_json := jsonb_build_object(
            'event', cmd.command_tag,
            'schema_name', table_schema,
            'table_name', table_name,
            'table_id', table_oid,
            'columns', schema_json
        );

        perform pg_logical_emit_message(
            transactional => true,
            prefix        => 'supabase_etl_ddl',
            content       => msg_json::text
        );
    end loop;

    return;
end;
$$;

create event trigger etl_ddl_message_trigger
    on ddl_command_end
    when tag = 'ALTER TABLE'
    execute function etl.emit_schema_change_messages();
