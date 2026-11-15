-- Report a column as not null only when its not-null constraint is validated.
--
-- PostgreSQL 18 accepts `alter table ... add constraint ... not null c not
-- valid`. It sets `pg_attribute.attnotnull` without scanning the table, so rows
-- written before the constraint keep their nulls until `validate constraint`
-- runs. ETL read `attnotnull` alone and marked the column non-nullable. A full
-- old row from an update or delete of such a row then carried a null that the
-- decoder rejects as protocol corruption, stopping the apply worker on every
-- retry, and destinations created the column as not null while the initial copy
-- still sent those nulls.
--
-- The column now stays nullable while its not-null constraint is unvalidated.
-- New rows are still checked by PostgreSQL, so only the existing nulls flow
-- through. Before PostgreSQL 18 `pg_constraint` has no not-null rows, so the
-- result is unchanged there. Only the `attnotnull` expression changes; the
-- signature, result columns, settings, owner, grants and comment are kept. The
-- helper is stable and reads catalogs per call, so no backfill is needed.

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
    a.attnotnull and not exists (
        select 1
        from pg_catalog.pg_constraint con
        where con.conrelid = a.attrelid
          and con.contype = 'n'
          and con.conkey = array[a.attnum]
          and not con.convalidated
    ),
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
