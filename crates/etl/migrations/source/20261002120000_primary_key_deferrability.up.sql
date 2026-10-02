-- Report whether the effective primary key is DEFERRABLE.
--
-- A deferrable key can hold duplicate values until its constraint check runs,
-- so one statement may move a row into a key before the key's previous row
-- moves out. Destinations that key current state on the primary key cannot
-- apply that event order safely and need this flag to reject the table.
--
-- `create or replace` keeps the function's privileges, and the event trigger
-- keeps calling it by name. Messages emitted before this migration omit the
-- new field, and ETL reads them as not deferrable.

create or replace function etl.describe_table_identity(
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
-- Uses the same leaf-or-parent fallback as `effective_primary_key_cols`.
effective_primary_key_condeferrable as (
    select con.condeferrable
    from pg_catalog.pg_constraint con
    where con.conrelid = p_table
      and con.contype = 'p'
    union all
    select con.condeferrable
    from direct_parent dp
    join pg_catalog.pg_constraint con
      on con.conrelid = dp.parent_oid
     and con.contype = 'p'
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
    'primary_key_condeferrable',
    coalesce(
        (
            select pg_catalog.bool_or(epkd.condeferrable)
            from effective_primary_key_condeferrable epkd
        ),
        false
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

comment on function etl.describe_table_identity(pg_catalog.oid) is
$$Returns compact identity metadata for one table, including primary-key attnums,
whether that primary key is deferrable, and replica-identity state.

This helper intentionally avoids duplicating per-column identity annotations in
the payload. For partitions, it falls back to the direct parent primary key when
the leaf table does not expose a local primary-key constraint.$$;
