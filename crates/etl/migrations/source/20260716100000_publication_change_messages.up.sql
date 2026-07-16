-- Publication membership logical messages.
--
-- PostgreSQL exposes added publication mappings from `ddl_command_end` and
-- removed mappings from `sql_drop`. Both triggers call the same helper after
-- the DDL action has updated the catalogs, and the helper emits the complete
-- post-change membership. The ETL apply worker owns the previous membership
-- and computes additions and removals in WAL order.
--
-- The event-trigger entrypoints are SECURITY DEFINER so publication owners do
-- not need direct access to ETL helpers. Their search path resolves pg_catalog
-- first and places the per-session pg_temp schema last, helper EXECUTE
-- privileges are revoked from PUBLIC, and no dynamic SQL includes
-- user-controlled identifiers. The shared snapshot helper remains SECURITY
-- INVOKER and is reachable only through those entrypoints.
--
-- Runtime overhead is concentrated in ALTER PUBLICATION. Other drop-capable
-- DDL performs one scan of PostgreSQL's already-materialized dropped-object
-- set and returns immediately unless a publication mapping is present. Each
-- affected publication produces one compact OID-only snapshot, with no user
-- table scans or row-data access.

drop event trigger if exists supabase_etl_publication_change_ddl_trigger;
drop event trigger if exists supabase_etl_publication_change_drop_trigger;

drop function if exists etl.emit_publication_ddl_change_message();
drop function if exists etl.emit_publication_drop_change_message();
drop function if exists etl.emit_publication_change_message(pg_catalog.text);

create function etl.emit_publication_change_message(
    p_publication_name pg_catalog.text
) returns pg_catalog.void
language plpgsql
security invoker
set search_path = pg_catalog, pg_temp
as
$fnc$
declare
    v_message pg_catalog.jsonb;
begin
    if coalesce(
        pg_catalog.current_setting('supabase_etl.skip_ddl_log', true),
        'false'
    )::pg_catalog.bool then
        return;
    end if;

    if pg_catalog.current_setting('wal_level', true) is distinct from 'logical' then
        return;
    end if;

    -- A dropped publication has no membership to reconcile. Pipelines report
    -- the missing publication through their normal source validation path.
    if not exists (
        select 1
        from pg_catalog.pg_publication p
        where p.pubname = p_publication_name
    ) then
        return;
    end if;

    select pg_catalog.jsonb_build_object(
        'publication_name', p_publication_name,
        'table_ids', coalesce(
            pg_catalog.jsonb_agg(
                publication_tables.table_id::pg_catalog.int8
                order by publication_tables.table_id
            ),
            '[]'::pg_catalog.jsonb
        )
    )
    into v_message
    from (
        select distinct publication_table.relid::pg_catalog.oid as table_id
        from pg_catalog.pg_get_publication_tables(p_publication_name) publication_table
    ) publication_tables;

    -- Keep the message in the ALTER PUBLICATION transaction so logical
    -- decoding exposes it only after commit and discards it on rollback.
    perform pg_catalog.pg_logical_emit_message(
        true,
        'supabase_etl_publication_change',
        pg_catalog.convert_to(v_message::pg_catalog.text, 'utf8')
    );
end;
$fnc$;

revoke all on function etl.emit_publication_change_message(pg_catalog.text) from public;

comment on function etl.emit_publication_change_message(pg_catalog.text) is
$$Emits one transactional logical message containing the complete post-change
table membership of a publication. Empty publications are represented by an
empty table_ids array. This SECURITY INVOKER helper has no PUBLIC execute
privilege and is called by the SECURITY DEFINER event-trigger entrypoints.$$;

create function etl.emit_publication_ddl_change_message()
returns pg_catalog.event_trigger
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as
$fnc$
declare
    v_publication_name pg_catalog.text;
begin
    for v_publication_name in
        select distinct affected.publication_name
        from (
            select case
                when command.classid = 'pg_catalog.pg_publication'::pg_catalog.regclass then
                    (address.object_names)[1]
                else
                    (address.object_args)[1]
            end as publication_name
            from pg_catalog.pg_event_trigger_ddl_commands() command
            cross join lateral pg_catalog.pg_identify_object_as_address(
                command.classid,
                command.objid,
                command.objsubid
            ) address
            where command.objid is not null
              and command.classid in (
                      'pg_catalog.pg_publication'::pg_catalog.regclass,
                      'pg_catalog.pg_publication_rel'::pg_catalog.regclass,
                      pg_catalog.to_regclass('pg_catalog.pg_publication_namespace')
                  )
              and not coalesce(command.in_extension, false)
        ) affected
        where affected.publication_name is not null
        order by affected.publication_name
    loop
        perform etl.emit_publication_change_message(v_publication_name);
    end loop;
end;
$fnc$;

revoke all on function etl.emit_publication_ddl_change_message() from public;

comment on function etl.emit_publication_ddl_change_message() is
$$Emits publication membership snapshots for mappings created or changed by
ALTER PUBLICATION after the command has updated the catalogs.$$;

create function etl.emit_publication_drop_change_message()
returns pg_catalog.event_trigger
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as
$fnc$
declare
    v_publication_name pg_catalog.text;
begin
    for v_publication_name in
        select distinct (dropped.address_args)[1]
        from pg_catalog.pg_event_trigger_dropped_objects() dropped
        where dropped.classid in (
                  'pg_catalog.pg_publication_rel'::pg_catalog.regclass,
                  pg_catalog.to_regclass('pg_catalog.pg_publication_namespace')
              )
          and (dropped.address_args)[1] is not null
        order by (dropped.address_args)[1]
    loop
        perform etl.emit_publication_change_message(v_publication_name);
    end loop;
end;
$fnc$;

revoke all on function etl.emit_publication_drop_change_message() from public;

comment on function etl.emit_publication_drop_change_message() is
$$Emits publication membership snapshots when explicit table or schema
publication mappings are removed, including removals caused by dropping a
published table or schema.$$;

create event trigger supabase_etl_publication_change_ddl_trigger
    on ddl_command_end
    when tag in ('ALTER PUBLICATION')
    execute function etl.emit_publication_ddl_change_message();

create event trigger supabase_etl_publication_change_drop_trigger
    on sql_drop
    execute function etl.emit_publication_drop_change_message();
