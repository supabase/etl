-- Keep pg_temp explicitly last so it cannot precede pg_catalog implicitly.
-- Catalog relations, types, and ETL helper calls remain schema-qualified.
-- Copy and CDC need column OIDs and modifiers, not formatted type names. Keep
-- describe_table_schema for older binaries and source introspection, while the
-- shared decoder query avoids its type-cache lookups and unused metadata.
create function etl.describe_table_columns(p_table pg_catalog.oid)
returns table (
    attname pg_catalog.text,
    attnum pg_catalog.int4,
    atttypid pg_catalog.oid,
    atttypmod pg_catalog.int4,
    attnotnull pg_catalog.bool,
    default_expression pg_catalog.text,
    missing_value_expression pg_catalog.text
)
language sql
stable
strict
set search_path = pg_catalog, pg_temp
-- Capture the configured migration connection's formats, matching copy.
set datestyle from current
set intervalstyle from current
set extra_float_digits from current
set timezone from current
set bytea_output from current
as
$fnc$
with column_values as (
    select
        a.attname::pg_catalog.text,
        a.attnum::pg_catalog.int4,
        a.atttypid,
        a.atttypmod,
        a.attnotnull,
        -- Defaults contain no column references; generated expressions are
        -- excluded below. OID 0 avoids opening and locking each table.
        case when a.atthasdef then pg_catalog.pg_get_expr(ad.adbin, 0) end as default_expression,
        a.atthasmissing,
        -- Partitioned tables store no rows or missing values; partitions keep
        -- the add-time values, so fall back to the current default.
        c.relkind <> 'p' and a.atttypid in (
            'pg_catalog.bool'::pg_catalog.regtype,
            'pg_catalog.int2'::pg_catalog.regtype, 'pg_catalog.int4'::pg_catalog.regtype,
            'pg_catalog.int8'::pg_catalog.regtype, 'pg_catalog.oid'::pg_catalog.regtype,
            'pg_catalog.float4'::pg_catalog.regtype,
            'pg_catalog.float8'::pg_catalog.regtype, 'pg_catalog.numeric'::pg_catalog.regtype,
            'pg_catalog.text'::pg_catalog.regtype, 'pg_catalog.varchar'::pg_catalog.regtype,
            'pg_catalog.bpchar'::pg_catalog.regtype, 'pg_catalog.name'::pg_catalog.regtype,
            'pg_catalog.char'::pg_catalog.regtype, 'pg_catalog.uuid'::pg_catalog.regtype,
            'pg_catalog.date'::pg_catalog.regtype, 'pg_catalog.time'::pg_catalog.regtype,
            'pg_catalog.timetz'::pg_catalog.regtype, 'pg_catalog.timestamp'::pg_catalog.regtype,
            'pg_catalog.timestamptz'::pg_catalog.regtype, 'pg_catalog.interval'::pg_catalog.regtype,
            'pg_catalog.json'::pg_catalog.regtype, 'pg_catalog.jsonb'::pg_catalog.regtype
        ) as missing_value_supported,
        a.attmissingval
    from pg_catalog.pg_attribute a
    join pg_catalog.pg_class c
      on c.oid = a.attrelid
    left join pg_catalog.pg_attrdef ad
      on ad.adrelid = a.attrelid and ad.adnum = a.attnum
    where a.attrelid = p_table and a.attnum > 0
      and not a.attisdropped and a.attgenerated = ''
), converted_values as materialized (
    select v.attname, v.attnum, v.atttypid, v.atttypmod, v.attnotnull,
           v.default_expression, v.atthasmissing, v.missing_value_supported,
           -- Generic JSON conversion can invoke untrusted casts. Do not run
           -- those with the SECURITY DEFINER trigger owner's privileges.
           case when v.missing_value_supported and v.atthasmissing
                then pg_catalog.array_to_json(v.attmissingval)->0 end as missing_value
    from column_values v
), literals as (
    select v.*,
        -- Quote like pg_get_expr: standard-conforming strings, never E''.
        case
            when not v.missing_value_supported then null
            when not v.atthasmissing then 'null'
            when v.atttypid in ('pg_catalog.json'::pg_catalog.regtype, 'pg_catalog.jsonb'::pg_catalog.regtype)
                then '''' || pg_catalog.replace(v.missing_value::pg_catalog.text, '''', '''''') || ''''
            -- A bare numeric -0 becomes positive zero before a float cast.
            when v.atttypid in ('pg_catalog.float4'::pg_catalog.regtype, 'pg_catalog.float8'::pg_catalog.regtype)
                then '''' || (v.missing_value #>> '{}') || ''''
            when pg_catalog.json_typeof(v.missing_value) in ('number', 'boolean')
                then v.missing_value::pg_catalog.text
            when pg_catalog.json_typeof(v.missing_value) = 'string'
                then '''' || pg_catalog.replace(v.missing_value #>> '{}', '''', '''''') || ''''
            -- Array/composite defaults retain the existing portability fallback.
        end as missing_literal
    from converted_values v
)
select l.attname, l.attnum, l.atttypid, l.atttypmod, l.attnotnull,
       l.default_expression,
       case when l.missing_literal is distinct from coalesce(l.default_expression, 'null')
            then l.missing_literal end
from literals l
order by l.attnum;
$fnc$;

revoke all on function etl.describe_table_columns(pg_catalog.oid) from public;

comment on function etl.describe_table_columns(pg_catalog.oid) is
$$Returns the non-generated columns needed by both copy and CDC decoders from
one MVCC catalog snapshot. Captured missing values are encoded as SQL literals
only when different from the current default. They describe fast column
additions, not values already materialized by rewrites or volatile defaults.
Partitioned tables report none because their partitions hold those values.
Default deparsing still uses pg_get_expr and cannot make concurrent shared-object
DDL or stale transaction snapshots safe.$$;

-- Capture protocol identity independently of destination primary-key metadata.
create or replace function etl.describe_table_identity(
    p_table pg_catalog.oid
) returns pg_catalog.jsonb
language sql
stable
strict
set search_path = pg_catalog, pg_temp
as
$fnc$
with rel as (
    select c.relreplident, c.relkind
    from pg_catalog.pg_class c
    where c.oid = p_table
),
-- Default identity is the table's own non-deferrable primary index. A
-- parent's key and included index attributes are not protocol identity keys.
protocol_identity_cols as (
    select x.attnum::pg_catalog.int4 as attnum, x.n as position
    from pg_catalog.pg_index i
    cross join rel r
    cross join lateral unnest(i.indkey) with ordinality as x(attnum, n)
    where i.indrelid = p_table and i.indislive and i.indimmediate
      -- Attaching an identity index to a deferrable constraint can leave
      -- indisreplident set; pgoutput still excludes non-immediate indexes.
      -- PostgreSQL 18 also selects an invalid partitioned primary index;
      -- earlier versions require validity for default replica identity too.
      and (i.indisvalid or (
          pg_catalog.current_setting('server_version_num')::pg_catalog.int4 >= 180000
          and r.relreplident = 'd' and r.relkind = 'p'
      ))
      and ((r.relreplident = 'd' and i.indisprimary)
           or (r.relreplident = 'i' and i.indisreplident))
      and x.n <= i.indnkeyatts and x.attnum > 0
),
protocol_identity as (
    select coalesce(pg_catalog.jsonb_agg(p.attnum order by p.position),
                    '[]'::pg_catalog.jsonb) as attnums
    from protocol_identity_cols p
),
replica_identity_index as (
    select ic.relname as index_name
    from pg_catalog.pg_index i
    join pg_catalog.pg_class ic
      on ic.oid = i.indexrelid
    where i.indrelid = p_table
      and i.indisreplident
)
select pg_catalog.jsonb_build_object(
    'primary_key_attnums',
    coalesce(pg_catalog.to_jsonb(pk.conkey), '[]'::pg_catalog.jsonb),
    'primary_key_condeferrable',
    coalesce(pk.condeferrable, false),
    'replica_identity_attnums',
    pi.attnums,
    'relreplident',
    r.relreplident::pg_catalog.text,
    'replica_identity_index_relname',
    (
        select rii.index_name
        from replica_identity_index rii
        limit 1
    ),
    -- Older binaries still read this field for explicit-index identity.
    'replica_identity_index_attnums',
    case when r.relreplident = 'i' then pi.attnums else '[]'::pg_catalog.jsonb end
)
from rel r
cross join protocol_identity pi
left join pg_catalog.pg_constraint pk
  on pk.conrelid = p_table and pk.contype = 'p';
$fnc$;

comment on function etl.describe_table_identity(pg_catalog.oid) is
$$Returns primary-key metadata and the table-local protocol replica-identity
keys from one MVCC catalog snapshot. Protocol keys exclude non-immediate
indexes and non-key included index columns. Primary-key metadata comes from
the table's own constraint, including partition-local attribute numbers;
a parent-only constraint does not invent a primary key on its partitions.$$;

-- Give copy and streaming one MVCC definition of publication columns.
-- Schema, identity, and these columns are captured in one trigger query.
-- Existing WAL payloads remain valid through the legacy relation fallback.

create function etl.describe_table_publication_columns(
    p_table pg_catalog.oid,
    p_publication pg_catalog.text default null
) returns table (publication_name pg_catalog.text, column_attnums pg_catalog.int2[])
language plpgsql
stable
set search_path = pg_catalog, pg_temp
as
$fnc$
begin
    -- PostgreSQL 14 has neither column lists nor schema publications. Keep
    -- version-specific catalogs in separately planned branches.
    -- Only declarative partition ancestors contribute inherited membership.
    -- 16384 is PostgreSQL's FirstNormalObjectId. Its publication expansion
    -- excludes initdb objects, including preinstalled information_schema
    -- tables. Mirror that rule without a current-cache publishability helper.
    if pg_catalog.current_setting('server_version_num')::pg_catalog.int4 >= 150000 then
        return query
        with recursive ancestors as (
            select c.oid, c.relnamespace, c.relispartition
            from pg_catalog.pg_class c where c.oid = p_table
            union all
            select c.oid, c.relnamespace, c.relispartition
            from ancestors a
            join pg_catalog.pg_inherits i on i.inhrelid = a.oid
            join pg_catalog.pg_class c on c.oid = i.inhparent
            where a.relispartition and not i.inhdetachpending
        ), published_ancestors as (
            select a.oid, pr.prpubid as pubid
            from ancestors a
            join pg_catalog.pg_publication_rel pr on pr.prrelid = a.oid
            union
            select a.oid, p.oid
            from ancestors a join pg_catalog.pg_publication p on p.puballtables
            union
            select a.oid, pn.pnpubid
            from ancestors a
            join pg_catalog.pg_publication_namespace pn on pn.pnnspid = a.relnamespace
        )
        select p.pubname::pg_catalog.text,
               case when not p.puballtables and pr.prattrs is not null
                         and not exists (
                             select 1 from pg_catalog.pg_publication_namespace pn
                             where pn.pnpubid = p.oid and pn.pnnspid = c.relnamespace
                         )
                    then pr.prattrs::pg_catalog.int2[]
                    else null::pg_catalog.int2[]
               end
        from (select distinct pa.pubid from published_ancestors pa) candidates
        join pg_catalog.pg_publication p on p.oid = candidates.pubid
        join pg_catalog.pg_class c on c.oid = p_table
        left join pg_catalog.pg_publication_rel pr
          on pr.prpubid = p.oid and pr.prrelid = c.oid
        where (p_publication is null or p.pubname::pg_catalog.text = p_publication)
          and c.relkind in ('r', 'p') and c.relpersistence = 'p'
          and c.oid >= 16384
          and ((not p.pubviaroot and c.relkind != 'p')
               or (p.pubviaroot and not exists (
                   select 1 from published_ancestors pa
                   where pa.pubid = p.oid and pa.oid != c.oid
               )));
    else
        return query
        with recursive ancestors as (
            select c.oid, c.relnamespace, c.relispartition
            from pg_catalog.pg_class c where c.oid = p_table
            union all
            select c.oid, c.relnamespace, c.relispartition
            from ancestors a
            join pg_catalog.pg_inherits i on i.inhrelid = a.oid
            join pg_catalog.pg_class c on c.oid = i.inhparent
            where a.relispartition and not i.inhdetachpending
        ), published_ancestors as (
            select a.oid, pr.prpubid as pubid
            from ancestors a
            join pg_catalog.pg_publication_rel pr on pr.prrelid = a.oid
            union
            select a.oid, p.oid
            from ancestors a join pg_catalog.pg_publication p on p.puballtables
        )
        select p.pubname::pg_catalog.text,
               null::pg_catalog.int2[]
        from (select distinct pa.pubid from published_ancestors pa) candidates
        join pg_catalog.pg_publication p on p.oid = candidates.pubid
        join pg_catalog.pg_class c on c.oid = p_table
        where (p_publication is null or p.pubname::pg_catalog.text = p_publication)
          and c.relkind in ('r', 'p') and c.relpersistence = 'p'
          and c.oid >= 16384
          and ((not p.pubviaroot and c.relkind != 'p')
               or (p.pubviaroot and not exists (
                   select 1 from published_ancestors pa
                   where pa.pubid = p.oid and pa.oid != c.oid
               )));
    end if;
end;
$fnc$;

revoke all on function etl.describe_table_publication_columns(pg_catalog.oid, pg_catalog.text) from public;

comment on function etl.describe_table_publication_columns(pg_catalog.oid, pg_catalog.text) is
$$Returns the effective column filter for a table in one or all
publications using the caller's MVCC catalog snapshot. Copy and streaming DDL
capture share the same membership, partition-root, and column-list rules.
A null column_attnums value means all supported table columns; explicit
filters retain PostgreSQL attnums without resolving names for every publication.
No rows means the relation is not an effective publication target. The helper
does not expand publications through current-cache functions or lock unrelated
tables. It does not make concurrent topology or publication changes supported.$$;

-- Bound publication-wide DDL discovery before reading complete table snapshots.
create function etl.publication_table_candidates(p_publication pg_catalog.oid)
returns table (table_oid pg_catalog.oid)
language plpgsql
stable
set search_path = pg_catalog, pg_temp
as
$fnc$
declare
    v_schema_oids pg_catalog.oid[];
begin
    -- PostgreSQL 14 must not plan a query against the newer schema catalog.
    if pg_catalog.current_setting('server_version_num')::pg_catalog.int4 >= 150000 then
        select pg_catalog.array_agg(pn.pnnspid) into v_schema_oids
        from pg_catalog.pg_publication_namespace pn
        where pn.pnpubid = p_publication;
    end if;

    return query
    with recursive roots as (
        select pr.prrelid as table_oid
        from pg_catalog.pg_publication_rel pr
        where pr.prpubid = p_publication
        union
        select c.oid
        from pg_catalog.pg_publication p
        join pg_catalog.pg_class c on p.puballtables or c.relnamespace = any(v_schema_oids)
        where p.oid = p_publication
          and (p.puballtables or v_schema_oids is not null)
          and c.relkind in ('r', 'p') and c.relpersistence = 'p'
          -- FirstNormalObjectId, matching PostgreSQL's publication expansion.
          and c.oid >= 16384
    ), candidates as (
        select r.table_oid from roots r
        union
        select i.inhrelid
        from candidates p
        join pg_catalog.pg_inherits i on i.inhparent = p.table_oid
        join pg_catalog.pg_class c on c.oid = i.inhrelid
        where c.relispartition and not i.inhdetachpending
    )
    select c.table_oid from candidates c;
end;
$fnc$;

revoke all on function etl.publication_table_candidates(pg_catalog.oid) from public;

comment on function etl.publication_table_candidates(pg_catalog.oid) is
$$Returns snapshot-visible table candidates for one publication using direct,
schema, and all-table membership, including declarative descendants. This only
bounds discovery: describe_table_publication_columns remains authoritative for
effective root/leaf targets and filters. Catalog reads use the caller's MVCC
snapshot and do not lock unrelated user tables.$$;

-- Preserve non-PUBLIC readers when an admin installs helpers on behalf of
-- a non-owner pipeline role. New signatures do not inherit existing grants.
-- This runs once during installation, not for each captured DDL command.
do
$acl$
declare
    r record;
    v_trigger_owner pg_catalog.text;
begin
    for r in
        select roles.rolname, pg_catalog.bool_or(acl.is_grantable) as is_grantable
        from pg_catalog.pg_proc p
        cross join lateral pg_catalog.aclexplode(
            coalesce(p.proacl, pg_catalog.acldefault('f', p.proowner))
        ) acl
        join pg_catalog.pg_roles roles on roles.oid = acl.grantee
        where p.oid = 'etl.describe_table_schema(pg_catalog.oid)'::pg_catalog.regprocedure
          and acl.privilege_type = 'EXECUTE'
        group by roles.rolname
    loop
        execute pg_catalog.format(
            'grant execute on function etl.describe_table_columns(pg_catalog.oid), '
            'etl.describe_table_publication_columns(pg_catalog.oid, pg_catalog.text) to %I%s',
            r.rolname,
            case when r.is_grantable then ' with grant option' else '' end
        );
    end loop;

    -- Replacement retains the trigger function's owner, which may differ
    -- from this installer. Only that trusted owner needs the private helper.
    select roles.rolname into v_trigger_owner
    from pg_catalog.pg_proc p
    join pg_catalog.pg_roles roles on roles.oid = p.proowner
    where p.oid = 'etl.emit_schema_change_messages()'::pg_catalog.regprocedure;
    execute pg_catalog.format(
        'grant execute on function etl.publication_table_candidates(pg_catalog.oid) to %I',
        v_trigger_owner
    );
end;
$acl$;

drop event trigger if exists supabase_etl_ddl_message_trigger;

create or replace function etl.emit_schema_change_messages()
returns pg_catalog.event_trigger
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as
$fnc$
declare
    r record;
    v_msg_json pg_catalog.jsonb;
begin
    if coalesce(pg_catalog.current_setting('supabase_etl.skip_ddl_log', true), 'false')::pg_catalog.bool then
        return;
    end if;

    -- Without logical WAL there is no downstream consumer for emitted messages.
    if pg_catalog.current_setting('wal_level', true) is distinct from 'logical' then
        return;
    end if;

    for r in
        with recursive event_commands as (
            select
                d.classid,
                d.objid,
                d.object_type
            from pg_catalog.pg_event_trigger_ddl_commands() d
            where d.objid is not null
              and not coalesce(d.in_extension, false)
        ),
        base as (
            -- Table DDL is unscoped. Per-table publication commands identify a
            -- surviving pg_publication_rel row with both target and publication.
            select
                coalesce(pr.prrelid, d.objid) as table_oid,
                p.pubname as publication_name
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
            -- Publication-level commands identify pg_publication. Expand its
            -- post-command members without parsing submitted SQL.
            select
                c.oid as table_oid,
                p.pubname as publication_name
            from event_commands d
            join pg_catalog.pg_publication p
              on d.classid = 'pg_catalog.pg_publication'::pg_catalog.regclass
             and d.objid = p.oid
            cross join lateral etl.publication_table_candidates(p.oid) candidate
            join pg_catalog.pg_class c on c.oid = candidate.table_oid
        ),
        -- A table DDL refreshes the whole containing declarative partition
        -- tree. Ancestors can be published identities even for leaf-local DDL.
        -- Ordinary inheritance retains its existing downward-only behavior.
        partition_ancestors as (
            select b.* from base b
            union
            select i.inhparent, a.publication_name
            from partition_ancestors a
            join pg_catalog.pg_class c on c.oid = a.table_oid and c.relispartition
            join pg_catalog.pg_inherits i on i.inhrelid = a.table_oid
            where a.publication_name is null and not i.inhdetachpending
        ),
        -- UNION emits each identity once, including across inheritance diamonds.
        -- Publication DDL already supplies its scoped effective identities.
        affected as (
            select a.* from partition_ancestors a
            union
            select
                i.inhrelid,
                b.publication_name
            from affected b
            join pg_catalog.pg_inherits i on i.inhparent = b.table_oid
            where b.publication_name is null and not i.inhdetachpending
        )
        select
            c.oid as table_oid,
            n.nspname,
            c.relname,
            ddl.publication_name,
            ss.columns,
            etl.describe_table_identity(c.oid) as identity,
            pc.publication_column_attnums
        from affected ddl
        join pg_catalog.pg_class c
          on c.oid = ddl.table_oid
        join pg_catalog.pg_namespace n
          on n.oid = c.relnamespace
        cross join lateral (
            select pg_catalog.jsonb_object_agg(
                p.publication_name, pg_catalog.to_jsonb(p.column_attnums)
            ) as publication_column_attnums
            from etl.describe_table_publication_columns(c.oid, ddl.publication_name::pg_catalog.text) p
        ) pc
        cross join lateral (
            select coalesce(pg_catalog.jsonb_agg(
                pg_catalog.jsonb_build_object(
                    'attname', s.attname,
                    'attnum', s.attnum,
                    'atttypid', s.atttypid::pg_catalog.int8,
                    'atttypmod', s.atttypmod,
                    'attnotnull', s.attnotnull,
                    'default_expression', s.default_expression
                ) || case when s.missing_value_expression is null then '{}'::pg_catalog.jsonb
                          else pg_catalog.jsonb_build_object('missing_value_expression', s.missing_value_expression)
                     end order by s.attnum
            ), '[]'::pg_catalog.jsonb) as columns
            from etl.describe_table_columns(c.oid) s
            where pc.publication_column_attnums is not null
        ) ss
        where c.relkind in ('r', 'p') and c.relpersistence = 'p'
          and pc.publication_column_attnums is not null
        -- All payload fields use this one statement's catalog snapshot.
        order by c.oid
    loop
        v_msg_json := pg_catalog.jsonb_build_object(
            'command_tag', tg_tag,
            'nspname', r.nspname,
            'relname', r.relname,
            'oid', r.table_oid::pg_catalog.int8,
            'identity', r.identity,
            'columns', r.columns,
            'publication_column_attnums', r.publication_column_attnums
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
$$Emits transactional schema snapshots for effective published permanent tables.
Table DDL refreshes each identity in the containing declarative partition tree
using that table's own schema; ordinary inheritance expands downward only.
Publication DDL describes surviving targets scoped to the changed publication.
Removed membership rows and other catalog object types emit no snapshots.

Schema, identity, and publication filters share one statement snapshot through
stable helpers. Payloads omit submitted SQL and command descriptions. Messages
roll back with DDL and retain statement order. Capture does not synchronize rows
for membership or partition-topology changes. The trusted function owner supplies
helper privileges, so table owners need no direct helper grants.$$;

create event trigger supabase_etl_ddl_message_trigger
    on ddl_command_end
    when tag in ('ALTER TABLE', 'ALTER PUBLICATION')
    execute function etl.emit_schema_change_messages();
