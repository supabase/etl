drop event trigger if exists supabase_etl_ddl_message_trigger;

create or replace function etl.emit_schema_change_messages()
returns pg_catalog.event_trigger
language plpgsql
security definer
set search_path = pg_catalog
as
$fnc$
declare
    r record;
    v_schema_json pg_catalog.jsonb;
    v_identity_json pg_catalog.jsonb;
    v_msg_json pg_catalog.jsonb;
    v_statement_text pg_catalog.text;
begin
    if coalesce(pg_catalog.current_setting('supabase_etl.skip_ddl_log', true), 'false')::pg_catalog.bool then
        return;
    end if;

    if pg_catalog.current_setting('wal_level', true) is distinct from 'logical' then
        return;
    end if;

    v_statement_text := pg_catalog.current_query();

    for r in
        with base as (
            select
                d.classid,
                d.objid,
                d.objsubid,
                d.command_tag,
                d.object_type,
                d.schema_name,
                d.object_identity,
                addr.type as object_address_type,
                addr.object_names as object_address_names,
                addr.object_args as object_address_args
            from pg_catalog.pg_event_trigger_ddl_commands() d
            left join lateral pg_catalog.pg_identify_object_as_address(
                d.classid,
                d.objid,
                d.objsubid
            )
                as addr(type, object_names, object_args)
                on true
            where d.objid is not null
              and d.object_type in ('table', 'table column')
              and not coalesce(d.in_extension, false)
        ),
        ddl as (
            select
                b.objid,
                pg_catalog.jsonb_agg(
                    pg_catalog.jsonb_build_object(
                        'classid', b.classid::pg_catalog.int8,
                        'objid', b.objid::pg_catalog.int8,
                        'objsubid', b.objsubid,
                        'command_tag', b.command_tag,
                        'object_type', b.object_type,
                        'schema_name', b.schema_name,
                        'object_identity', b.object_identity,
                        'object_address_type', b.object_address_type,
                        'object_address_names', b.object_address_names,
                        'object_address_args', b.object_address_args
                    )
                    order by
                        b.objsubid,
                        b.classid,
                        b.command_tag,
                        b.object_type,
                        b.schema_name,
                        b.object_identity
                ) as commands
            from base b
            group by b.objid
        )
        select
            c.oid as table_oid,
            n.nspname,
            c.relname,
            c.relkind::pg_catalog.text as relkind,
            ddl.commands
        from ddl
        join pg_catalog.pg_class c
          on c.oid = ddl.objid
        join pg_catalog.pg_namespace n
          on n.oid = c.relnamespace
        where c.relkind in ('r', 'p')
          and c.relpersistence = 'p'
          and exists (
              select 1
              from pg_catalog.pg_publication_tables pt
              where pt.schemaname = n.nspname
                and pt.tablename = c.relname
          )
    loop
        select pg_catalog.jsonb_agg(
            pg_catalog.jsonb_build_object(
                'attname', s.attname,
                'attnum', s.attnum,
                'atttypid', s.atttypid::pg_catalog.int8,
                'typname', s.typname,
                'formatted_type', s.formatted_type,
                'atttypmod', s.atttypmod,
                'attnotnull', s.attnotnull,
                'atthasdef', s.atthasdef,
                'default_expression', s.default_expression,
                'attidentity', s.attidentity,
                'atthasmissing', s.atthasmissing
            )
            order by s.attnum
        )
        into v_schema_json
        from etl.describe_table_schema(r.table_oid) s;

        if v_schema_json is null then
            continue;
        end if;

        select etl.describe_table_identity(r.table_oid)
        into v_identity_json;

        v_msg_json := pg_catalog.jsonb_build_object(
            'trigger_event', tg_event,
            'command_tag', tg_tag,
            'current_query', v_statement_text,
            'current_database', pg_catalog.current_database(),
            'server_version_num', pg_catalog.current_setting('server_version_num')::pg_catalog.int4,
            'nspname', r.nspname,
            'relname', r.relname,
            'oid', r.table_oid::pg_catalog.int8,
            'relkind', r.relkind,
            'commands', r.commands,
            'identity', v_identity_json,
            'columns', v_schema_json
        );

        perform pg_catalog.pg_logical_emit_message(
            true,
            'supabase_etl_ddl',
            pg_catalog.convert_to(v_msg_json::pg_catalog.text, 'utf8')
        );
    end loop;
end;
$fnc$;

revoke all on function etl.emit_schema_change_messages() from public;

comment on function etl.emit_schema_change_messages() is
$$Event trigger function that emits one logical schema-change message per
affected published permanent table for supported ALTER TABLE statements.

The payload is intentionally richer than what the application consumes today so
it can serve as a PostgreSQL-shaped source snapshot for future evolution. This
function runs with the privileges of its owner so table owners do not need
direct execute access to ETL helper functions when they run ALTER TABLE. It
also intentionally avoids PL/pgSQL EXCEPTION handlers to keep
pg_logical_emit_message() in the top-level transaction and preserve the expected
ordering of DDL messages relative to relation and DML events.$$;

create event trigger supabase_etl_ddl_message_trigger
    on ddl_command_end
    when tag in ('ALTER TABLE')
    execute function etl.emit_schema_change_messages();
