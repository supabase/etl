-- Render column defaults with fixed output settings.
--
-- `pg_get_expr` prints constants with the session's output settings. The DDL
-- event trigger calls this function in the session that ran the DDL, so a user
-- with `DateStyle = 'German'` sent `'02.01.2026'::date` for `'2026-01-02'`,
-- which ETL reads as February 1. `IntervalStyle`, `extra_float_digits`,
-- `TimeZone` and `bytea_output` changed defaults in the same way.
-- `standard_conforming_strings` and `quote_all_identifiers` change how literals
-- and type names are quoted.
--
-- Function-level `set` clauses pin the settings ETL connections already use, so
-- the DDL message and the bootstrap snapshot carry the same text. The settings
-- apply only while the function runs and are restored on return. Only the
-- `set` clauses change; the signature, result columns, body, owner, grants and
-- comment are kept, so no dependent object needs to be recreated.

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
