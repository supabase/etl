-- Schema change logical messages (DDL)
-- Adds helpers and trigger to emit logical decoding messages when tables change.

create or replace function etl.describe_table_schema(
    p_table oid
) returns table (
    column_name text,
    column_order int,
    column_type text,
    type_oid oid,
    type_modifier int,
    nullable boolean,
    primary_key_order int
)
language plpgsql
stable
as
$$
declare
    v_query text;
begin
    v_query := format(
        'with direct_parent as (
            select i.inhparent as parent_oid
            from pg_inherits i
            where i.inhrelid = %1$s
            limit 1
        ),
        primary_key as (
            select x.attnum, x.n as position
            from pg_constraint con
            join unnest(con.conkey) with ordinality as x(attnum, n) on true
            where con.contype = ''p''
              and con.conrelid = %1$s
        ),
        parent_primary_key as (
            select a.attname, x.n as position
            from pg_constraint con
            join unnest(con.conkey) with ordinality as x(attnum, n) on true
            join pg_attribute a on a.attrelid = con.conrelid and a.attnum = x.attnum
            join direct_parent dp on dp.parent_oid = con.conrelid
            where con.contype = ''p''
        )
        select
            a.attname::text,
            a.attnum::int,
            format_type(a.atttypid, a.atttypmod),
            a.atttypid,
            a.atttypmod,
            not a.attnotnull,
            coalesce(pk.position, ppk.position)::int
        from pg_attribute a
                 left join primary_key pk on pk.attnum = a.attnum
                 left join parent_primary_key ppk on ppk.attname = a.attname
        where a.attrelid = %1$s
          and a.attnum > 0
          and not a.attisdropped
          and a.attgenerated = ''''
        order by a.attnum;',
        p_table
    );

    return query execute v_query;
end;
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

        select jsonb_agg(
                   jsonb_build_object(
                       'column_name', s.column_name,
                       'column_order', s.column_order,
                       'column_type', s.column_type,
                       'type_oid', s.type_oid,
                       'type_modifier', s.type_modifier,
                       'nullable', s.nullable,
                       'primary_key_order', s.primary_key_order
                   )
               )
        into schema_json
        from etl.describe_table_schema(table_oid) s;

        if schema_json is null then
            continue;
        end if;

        msg_json := jsonb_build_object(
            'event', cmd.command_tag,
            'schema_name', table_schema,
            'table_name', table_name,
            'table_id', table_oid::bigint,
            'columns', schema_json
        );

        perform pg_logical_emit_message(
            true,
            'supabase_etl_ddl',
            convert_to(msg_json::text, 'utf8')
        );
    end loop;
end;
$$;

drop event trigger if exists etl_ddl_message_trigger;

create event trigger etl_ddl_message_trigger
    on ddl_command_end
    when tag in ('ALTER TABLE')
    execute function etl.emit_schema_change_messages();
