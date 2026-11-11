-- Render column defaults with fixed output settings, and report each column's
-- base type.
--
-- `pg_get_expr` prints constants with the session's output settings. The DDL
-- event trigger calls `etl.describe_table_schema` in the session that ran the
-- DDL, so a user with `DateStyle = 'German'` sent `'02.01.2026'::date` for
-- `'2026-01-02'`, which ETL reads as February 1. `IntervalStyle`,
-- `extra_float_digits`, `TimeZone` and `bytea_output` changed defaults in the
-- same way. `standard_conforming_strings` and `quote_all_identifiers` change
-- how literals and type names are quoted. Function-level `set` clauses pin the
-- settings ETL connections already use, so the DDL message and the bootstrap
-- snapshot carry the same text. The settings apply only while the function runs
-- and are restored on return. Only the `set` clauses change; the signature,
-- result columns, body, owner, grants and comment are kept.
--
-- A domain default renders as a value of the domain's base type, before the
-- domain's type modifier rounds or pads it. ETL needs the base type and its
-- modifier to tell whether the default is the value Postgres stores. The new
-- `etl.describe_base_type` resolves domain chains the way Postgres does, and
-- `etl.emit_schema_change_messages` adds its result to every column. The
-- trigger function keeps its signature, so `create or replace` keeps its owner,
-- grants, comment and event trigger. ETL's bootstrap snapshot resolves the base
-- type with its own query, so pipelines whose source objects predate this
-- migration keep working.

create or replace function etl.describe_table_schema(
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
set datestyle = 'ISO'
set intervalstyle = 'postgres'
set extra_float_digits = 3
set timezone = 'UTC'
set bytea_output = 'hex'
set standard_conforming_strings = on
set quote_all_identifiers = off
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

create function etl.describe_base_type(
    p_type pg_catalog.oid,
    p_typmod pg_catalog.int4
) returns table (
    base_type_oid pg_catalog.oid,
    base_typmod pg_catalog.int4,
    base_formatted_type pg_catalog.text
)
language sql
stable
strict
set search_path = pg_catalog
set quote_all_identifiers = off
as
$fnc$
-- Each domain step takes the domain's `typtypmod`, as `getBaseTypeAndTypmod`
-- does. Postgres rejects domain cycles; the depth bound is a safety net.
with recursive chain(type_oid, typmod, depth) as (
    select p_type, p_typmod, 0
    union all
    select t.typbasetype, t.typtypmod, c.depth + 1
    from chain c
    join pg_catalog.pg_type t
      on t.oid = c.type_oid
    where t.typtype = 'd'
      and c.depth < 64
)
select
    c.type_oid,
    c.typmod,
    pg_catalog.format_type(c.type_oid, c.typmod)::pg_catalog.text
from chain c
order by c.depth desc
limit 1;
$fnc$;

revoke all on function etl.describe_base_type(pg_catalog.oid, pg_catalog.int4) from public;

comment on function etl.describe_base_type(pg_catalog.oid, pg_catalog.int4) is
$$Returns the base type, type modifier and `format_type` output of a type, after
resolving domains. Non-domain types return themselves.$$;

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

    -- Without logical WAL there is no downstream consumer for emitted messages.
    if pg_catalog.current_setting('wal_level', true) is distinct from 'logical' then
        return;
    end if;

    -- `current_query()` is for observability only. It is the client-submitted
    -- text and may include more than one statement.
    v_statement_text := pg_catalog.current_query();

    for r in
        with event_commands as (
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
              and not coalesce(d.in_extension, false)
        ),
        base as (
            -- ALTER TABLE identifies the relation directly and is intentionally
            -- unscoped: a table change applies to every publication containing
            -- that table. Per-table ALTER PUBLICATION events instead identify a
            -- surviving pg_publication_rel row, from which both relation and
            -- publication are resolved. This branch uses only the explicitly
            -- identified relation; it does not expand partition roots into their
            -- effective leaf relations.
            select
                coalesce(pr.prrelid, d.objid) as table_oid,
                d.classid,
                d.objid,
                d.objsubid,
                d.command_tag,
                d.object_type,
                d.schema_name,
                d.object_identity,
                p.pubname as publication_name,
                d.object_address_type,
                d.object_address_names,
                d.object_address_args
            from event_commands d
            left join pg_catalog.pg_publication_rel pr
              on d.classid = 'pg_catalog.pg_publication_rel'::pg_catalog.regclass
             and d.objid = pr.oid
            left join pg_catalog.pg_publication p
              on p.oid = pr.prpubid
            where (
                  d.object_type in ('table', 'table column')
                  or pr.prrelid is not null
              )
            union all
            -- Publication-level ALTER PUBLICATION events identify pg_publication
            -- itself rather than a specific relation. The command parameters are
            -- not inspected; every such event expands the publication's post-DDL
            -- effective table set so each table receives a scoped snapshot.
            select
                c.oid as table_oid,
                d.classid,
                d.objid,
                d.objsubid,
                d.command_tag,
                d.object_type,
                d.schema_name,
                d.object_identity,
                p.pubname as publication_name,
                d.object_address_type,
                d.object_address_names,
                d.object_address_args
            from event_commands d
            join pg_catalog.pg_publication p
              on d.classid = 'pg_catalog.pg_publication'::pg_catalog.regclass
             and d.objid = p.oid
            join pg_catalog.pg_publication_tables pt
              on pt.pubname = p.pubname
            join pg_catalog.pg_namespace n
              on n.nspname = pt.schemaname
            join pg_catalog.pg_class c
              on c.relnamespace = n.oid
             and c.relname = pt.tablename
        ),
        ddl as (
            select
                b.table_oid,
                b.publication_name,
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
            group by b.table_oid, b.publication_name
        )
        select
            c.oid as table_oid,
            n.nspname,
            c.relname,
            c.relkind::pg_catalog.text as relkind,
            ddl.publication_name,
            ddl.commands
        from ddl
        join pg_catalog.pg_class c
          on c.oid = ddl.table_oid
        join pg_catalog.pg_namespace n
          on n.oid = c.relnamespace
        where c.relkind in ('r', 'p')
          and c.relpersistence = 'p'
          and exists (
              -- Keep only relations that are effective in at least one
              -- publication in this transaction's post-command catalog state.
              select 1
              from pg_catalog.pg_publication_tables pt
              where pt.schemaname = n.nspname
                and pt.tablename = c.relname
          )
        -- Make multi-table schema-message order deterministic.
        order by c.oid
    loop
        select pg_catalog.jsonb_agg(
            pg_catalog.jsonb_build_object(
                'attname', s.attname,
                'attnum', s.attnum,
                'atttypid', s.atttypid::pg_catalog.int8,
                'typname', s.typname,
                'formatted_type', s.formatted_type,
                'atttypmod', s.atttypmod,
                'base_type_oid', b.base_type_oid::pg_catalog.int8,
                'base_typmod', b.base_typmod,
                'base_formatted_type', b.base_formatted_type,
                'attnotnull', s.attnotnull,
                'atthasdef', s.atthasdef,
                'default_expression', s.default_expression,
                'attidentity', s.attidentity,
                'atthasmissing', s.atthasmissing
            )
            order by s.attnum
        )
        into v_schema_json
        from etl.describe_table_schema(r.table_oid) s
        cross join lateral etl.describe_base_type(s.atttypid, s.atttypmod) b;

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

        if r.publication_name is not null then
            v_msg_json := v_msg_json || pg_catalog.jsonb_build_object(
                'publication_name', r.publication_name
            );
        end if;

        perform pg_catalog.pg_logical_emit_message(
            true,
            'supabase_etl_ddl',
            pg_catalog.convert_to(v_msg_json::pg_catalog.text, 'utf8')
        );
    end loop;
end;
$fnc$;
