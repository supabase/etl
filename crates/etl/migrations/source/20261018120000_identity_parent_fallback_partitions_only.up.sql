-- Restrict the parent primary-key fallback to declarative partitions.
--
-- `direct_parent` previously matched any `pg_inherits` parent, so a legacy
-- inheritance child (`create table child () inherits (parent)`) reported its
-- parent's attnums despite not inheriting the constraint. ETL then classified
-- it `IdentityType::PrimaryKey` while PostgreSQL rejects its UPDATE and DELETE
-- outright. Requiring `relispartition` makes the guard match its documented
-- intent; it feeds `effective_primary_key_cols` and
-- `effective_primary_key_condeferrable` alike.
--
-- Behavior change: an affected child now reports `primary_key_attnums = []`,
-- so destinations requiring a source primary key reject it at copy start
-- instead of replicating it against a key it does not have. Declarative
-- partitions are unaffected. The helper is stable and reads catalogs per call,
-- so no backfill is needed.

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
    join pg_catalog.pg_class c
      on c.oid = i.inhrelid
    where i.inhrelid = p_table
      and c.relispartition
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
the payload. The direct-parent primary-key fallback applies only to declarative
partitions: a legacy inheritance child does not inherit its parent's
constraint, so the parent's key does not describe it.$$;
