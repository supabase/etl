-- Schema change logical messages (DDL).
--
-- This migration emits one logical message per changed replicated table for
-- supported `ALTER TABLE` statements.
--
-- The payload is intentionally broader than what the application consumes
-- today. It is meant to be a PostgreSQL-shaped snapshot of current catalog
-- state so downstream code can read only the fields it needs now and adopt
-- more fields later without redesigning the trigger. It is still a selective
-- snapshot rather than a full catalog dump: dropped columns and generated
-- columns are intentionally omitted.
--
-- Additional notes:
-- - `current_query` is the client-submitted text, may contain literals and
--   multiple statements, and is debug-only rather than replayable DDL.
-- - The trigger is read-mostly on purpose: it reads catalog state, builds the
--   payload, and emits one logical message.
-- - `supabase_etl.skip_ddl_log` exists as an emergency opt-out so a session can
--   bypass DDL logging if needed while the system is being recovered.
-- - These functions are dropped and recreated by design, so they become owned
--   by the role running the migration. If a deployment needs a different
--   owner, add explicit `ALTER FUNCTION ... OWNER TO ...` statements nearby.

drop event trigger if exists supabase_etl_ddl_message_trigger;

drop function if exists etl.emit_schema_change_messages();
drop function if exists etl.describe_table_identity(pg_catalog.oid);
drop function if exists etl.describe_table_schema(pg_catalog.oid);

create function etl.describe_table_schema(
    p_table pg_catalog.oid
) returns table (
    attname pg_catalog.text,
    attnum pg_catalog.int4,
    atttypid pg_catalog.oid,
    typname pg_catalog.text,
    formatted_type pg_catalog.text,
    atttypmod pg_catalog.int4,
    attnotnull pg_catalog.bool,
    atthasdef pg_catalog.bool,
    default_expression pg_catalog.text,
    attidentity pg_catalog.text,
    atthasmissing pg_catalog.bool
)
language sql
stable
strict
set search_path = pg_catalog
as
$fnc$
select
    a.attname::pg_catalog.text,
    a.attnum::pg_catalog.int4,
    a.atttypid,
    t.typname::pg_catalog.text,
    pg_catalog.format_type(a.atttypid, a.atttypmod)::pg_catalog.text,
    a.atttypmod::pg_catalog.int4,
    a.attnotnull,
    a.atthasdef,
    case
        when a.atthasdef then pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)::pg_catalog.text
        else null
    end,
    nullif(a.attidentity, '')::pg_catalog.text,
    a.atthasmissing
from pg_catalog.pg_attribute a
join pg_catalog.pg_type t
  on t.oid = a.atttypid
left join pg_catalog.pg_attrdef ad
  on ad.adrelid = a.attrelid
 and ad.adnum = a.attnum
where a.attrelid = p_table
  and a.attnum > 0
  and not a.attisdropped
  and a.attgenerated = ''
order by a.attnum;
$fnc$;

revoke all on function etl.describe_table_schema(pg_catalog.oid) from public;

comment on function etl.describe_table_schema(pg_catalog.oid) is
$$Returns the visible, non-generated column snapshot for one table after DDL has
been applied.

The result stays close to PostgreSQL catalog naming so the emitted JSON can act
as a source-native snapshot rather than an ETL-specific schema representation.$$;

create function etl.describe_table_identity(
    p_table pg_catalog.oid
) returns pg_catalog.jsonb
language sql
stable
strict
set search_path = pg_catalog
as
$fnc$
with rel as (
    select c.relreplident
    from pg_catalog.pg_class c
    where c.oid = p_table
),
direct_parent as (
    select i.inhparent as parent_oid
    from pg_catalog.pg_inherits i
    where i.inhrelid = p_table
    order by i.inhseqno
    limit 1
),
primary_key_cols as (
    select
        x.attnum::pg_catalog.int4 as attnum,
        x.n::pg_catalog.int4 as position
    from pg_catalog.pg_constraint con
    cross join lateral unnest(con.conkey) with ordinality as x(attnum, n)
    where con.conrelid = p_table
      and con.contype = 'p'
),
parent_primary_key_cols as (
    select
        x.attnum::pg_catalog.int4 as attnum,
        x.n::pg_catalog.int4 as position
    from direct_parent dp
    join pg_catalog.pg_constraint con
      on con.conrelid = dp.parent_oid
     and con.contype = 'p'
    cross join lateral unnest(con.conkey) with ordinality as x(attnum, n)
),
effective_primary_key_cols as (
    select
        pkc.attnum,
        pkc.position
    from primary_key_cols pkc
    union all
    select
        ppkc.attnum,
        ppkc.position
    from parent_primary_key_cols ppkc
    where not exists (
        select 1
        from primary_key_cols pkc
    )
),
replica_identity_index as (
    select ic.relname as index_name
    from pg_catalog.pg_index i
    join pg_catalog.pg_class ic
      on ic.oid = i.indexrelid
    where i.indrelid = p_table
      and i.indisreplident
),
replica_identity_cols as (
    select
        x.attnum::pg_catalog.int4 as attnum,
        x.n::pg_catalog.int4 as position
    from pg_catalog.pg_index i
    cross join lateral unnest(i.indkey) with ordinality as x(attnum, n)
    where i.indrelid = p_table
      and i.indisreplident
      and x.n <= i.indnkeyatts
      and x.attnum > 0
)
select pg_catalog.jsonb_build_object(
    'primary_key_attnums',
    coalesce(
        (
            select pg_catalog.jsonb_agg(epkc.attnum order by epkc.position)
            from effective_primary_key_cols epkc
        ),
        '[]'::pg_catalog.jsonb
    ),
    'relreplident',
    r.relreplident::pg_catalog.text,
    'replica_identity_index_relname',
    (
        select rii.index_name
        from replica_identity_index rii
        limit 1
    ),
    'replica_identity_index_attnums',
    coalesce(
        (
            select pg_catalog.jsonb_agg(ric.attnum order by ric.position)
            from replica_identity_cols ric
        ),
        '[]'::pg_catalog.jsonb
    )
)
from rel r;
$fnc$;

revoke all on function etl.describe_table_identity(pg_catalog.oid) from public;

comment on function etl.describe_table_identity(pg_catalog.oid) is
$$Returns compact identity metadata for one table, including primary-key attnums
and replica-identity state.

This helper intentionally avoids duplicating per-column identity annotations in
the payload. For partitions, it falls back to the direct parent primary key when
the leaf table does not expose a local primary-key constraint.$$;

create function etl.emit_schema_change_messages()
returns pg_catalog.event_trigger
language plpgsql
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

    -- Without logical WAL there is no downstream consumer for emitted messages.
    if pg_catalog.current_setting('wal_level', true) is distinct from 'logical' then
        return;
    end if;

    -- `current_query()` is for observability only. It is the client-submitted
    -- text and may include more than one statement.
    v_statement_text := pg_catalog.current_query();

    for r in
        with base as (
            -- PostgreSQL can return multiple base commands for one SQL
            -- statement. We keep them all and aggregate them into `commands[]`
            -- per affected table instead of trying to collapse them early.
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
            left join lateral pg_catalog.pg_identify_object_as_address(d.classid, d.objid, d.objsubid)
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
        -- Only emit messages for permanent published tables that participate in
        -- the replicated data model.
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

        -- Emit a source-shaped snapshot that is richer than the current
        -- application reader requires. Extra fields can be adopted later
        -- without redesigning the trigger.
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
function intentionally avoids PL/pgSQL EXCEPTION handlers to keep
pg_logical_emit_message() in the top-level transaction and preserve the expected
ordering of DDL messages relative to relation and DML events.$$;

-- Only `ALTER TABLE` is captured here by design.
--
-- If other `ddl_command_end` triggers coexist, PostgreSQL fires them in
-- alphabetical order by trigger name, so choose names intentionally.
create event trigger supabase_etl_ddl_message_trigger
    on ddl_command_end
    when tag in ('ALTER TABLE')
    execute function etl.emit_schema_change_messages();
