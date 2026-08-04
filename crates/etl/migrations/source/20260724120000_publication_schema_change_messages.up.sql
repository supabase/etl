-- Extend transactional schema snapshots to supported ALTER PUBLICATION events.

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

revoke all on function etl.emit_schema_change_messages() from public;

comment on function etl.emit_schema_change_messages() is
$$Event trigger function that emits transactional logical schema snapshots for
published permanent tables affected by supported ALTER TABLE and ALTER
PUBLICATION events.

ALTER TABLE events identify a relation directly and emit an unscoped snapshot.
Surviving pg_publication_rel events resolve one explicitly identified relation
and emit a snapshot scoped to that publication. Events identifying
pg_publication itself expand the publication's post-command effective table set
and emit one scoped snapshot per table. Other ALTER PUBLICATION catalog object
types and removed pg_publication_rel rows emit no snapshots.

The function inspects catalog state after each statement without parsing the
client-submitted query text. Messages are transactional, disappear if the outer
transaction rolls back, and retain statement order relative to relation and DML
events. The function runs with its owner's privileges so table owners do not
need direct execute access to ETL helper functions.$$;

create event trigger supabase_etl_ddl_message_trigger
    on ddl_command_end
    when tag in ('ALTER TABLE', 'ALTER PUBLICATION')
    execute function etl.emit_schema_change_messages();
