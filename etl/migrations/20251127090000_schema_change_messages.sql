-- Schema change logical messages (DDL)
-- Adds helpers and trigger to emit logical decoding messages when tables change.

create or replace function etl.describe_table_schema(
    p_table pg_catalog.oid
) returns table (
    name pg_catalog.text,
    type_oid pg_catalog.oid,
    type_name pg_catalog.text,
    type_modifier pg_catalog.int4,
    ordinal_position pg_catalog.int4,
    primary_key_ordinal_position pg_catalog.int4,
    nullable pg_catalog.bool
)
language sql
stable
set search_path = pg_catalog
as
$fnc$
with direct_parent as (
    select i.inhparent as parent_oid
    from pg_catalog.pg_inherits i
    where i.inhrelid = p_table
    limit 1
),
primary_key as (
    select x.attnum, x.n as position
    from pg_catalog.pg_constraint con
    cross join lateral unnest(con.conkey) with ordinality as x(attnum, n)
    where con.contype = 'p'
      and con.conrelid = p_table
),
parent_primary_key as (
    select a.attname, x.n as position
    from pg_catalog.pg_constraint con
    cross join lateral unnest(con.conkey) with ordinality as x(attnum, n)
    join pg_catalog.pg_attribute a on a.attrelid = con.conrelid and a.attnum = x.attnum
    join direct_parent dp on dp.parent_oid = con.conrelid
    where con.contype = 'p'
)
select
    a.attname::pg_catalog.text,
    a.atttypid,
    case
        when tn.nspname = 'pg_catalog' then t.typname
        else tn.nspname || '.' || t.typname
    end::pg_catalog.text,
    a.atttypmod::pg_catalog.int4,
    a.attnum::pg_catalog.int4,
    coalesce(pk.position, ppk.position)::pg_catalog.int4,
    not a.attnotnull
from pg_catalog.pg_attribute a
join pg_catalog.pg_type t on t.oid = a.atttypid
join pg_catalog.pg_namespace tn on tn.oid = t.typnamespace
left join primary_key pk on pk.attnum = a.attnum
left join parent_primary_key ppk on ppk.attname = a.attname
where a.attrelid = p_table
  and a.attnum > 0
  and not a.attisdropped
  and a.attgenerated = ''
order by a.attnum;
$fnc$;

create or replace function etl.emit_schema_change_messages()
returns event_trigger
language plpgsql
set search_path = pg_catalog
as
$fnc$
declare
    v_object_type pg_catalog.text;
    v_objid pg_catalog.oid;
    v_command_tag pg_catalog.text;
    v_table_schema pg_catalog.text;
    v_table_name pg_catalog.text;
    v_schema_json pg_catalog.jsonb;
    v_msg_json pg_catalog.jsonb;
    v_wal_level pg_catalog.text;
begin
    -- Check if logical replication is enabled; if not, silently skip.
    -- This prevents crashes when Supabase ETL is installed but wal_level != logical.
    v_wal_level := current_setting('wal_level', true);
    if v_wal_level is distinct from 'logical' then
        raise warning '[Supabase ETL] wal_level is %, not logical. Schema change will not be captured.', v_wal_level;
        return;
    end if;

    for v_object_type, v_objid, v_command_tag in
        select object_type, objid, command_tag from pg_event_trigger_ddl_commands()
    loop
        begin
            -- 'table' covers most ALTER TABLE operations (ADD/DROP COLUMN, ALTER TYPE, etc.)
            -- 'table column' is returned specifically for RENAME COLUMN operations
            if v_object_type not in ('table', 'table column') then
                continue;
            end if;

            if v_objid is null then
                continue;
            end if;

            select n.nspname, c.relname
            into v_table_schema, v_table_name
            from pg_catalog.pg_class c
            join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            where c.oid = v_objid
              and c.relkind in ('r', 'p');

            if v_table_schema is null or v_table_name is null then
                continue;
            end if;

            select pg_catalog.jsonb_agg(
                pg_catalog.jsonb_build_object(
                    'name', s.name,
                    'type_oid', s.type_oid::pg_catalog.int8,
                    'type_modifier', s.type_modifier,
                    'ordinal_position', s.ordinal_position,
                    'primary_key_ordinal_position', s.primary_key_ordinal_position,
                    'nullable', s.nullable
                )
            )
            into v_schema_json
            from etl.describe_table_schema(v_objid) s;

            if v_schema_json is null then
                continue;
            end if;

            v_msg_json := pg_catalog.jsonb_build_object(
                'event', v_command_tag,
                'schema_name', v_table_schema,
                'table_name', v_table_name,
                'table_id', v_objid::pg_catalog.int8,
                'columns', v_schema_json
            );

            perform pg_catalog.pg_logical_emit_message(
                true,
                'supabase_etl_ddl',
                pg_catalog.convert_to(v_msg_json::pg_catalog.text, 'utf8')
            );

        exception when others then
            -- Never crash customer DDL; log warning instead.
            raise warning using
                message = format('[Supabase ETL] emit_schema_change_messages failed for table %s: %s',
                                 coalesce(v_objid::pg_catalog.regclass::pg_catalog.text, 'unknown'), SQLERRM),
                detail = 'You may need to repeat this DDL command on the downstream to keep logical replication running.';
        end;
    end loop;
exception when others then
    -- Outer safety net.
    raise warning '[Supabase ETL] emit_schema_change_messages outer exception: %', SQLERRM;
end;
$fnc$;

drop event trigger if exists etl_ddl_message_trigger;

-- Only ALTER TABLE is captured because:
-- - CREATE TABLE: No need, since the initial schema is loaded during the first table copy operation.
-- - DROP TABLE: No need, since dropped tables are not supported right now.
-- This trigger focuses on schema changes to existing replicated tables.
create event trigger etl_ddl_message_trigger
    on ddl_command_end
    when tag in ('ALTER TABLE')
    execute function etl.emit_schema_change_messages();
