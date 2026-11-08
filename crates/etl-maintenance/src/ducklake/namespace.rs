//! Namespace-aware metadata lookup shared by writers and maintenance.

use pg_escape::quote_identifier;

/// Filters a leaf schema `s` by the optional namespace bound to `$3`.
///
/// JSON extraction also works on format 1.0, where parent_schema_id is absent.
/// Parent identity is resolved by ID, never by splitting a dotted schema name.
pub fn table_namespace_filter(metadata_schema: &str) -> String {
    let metadata_schema = quote_identifier(metadata_schema);
    format!(
        "(($3::text is null and to_jsonb(s)->>'parent_schema_id' is null) or exists (select 1 \
         from {metadata_schema}.ducklake_schema as parent where parent.schema_id = \
         (to_jsonb(s)->>'parent_schema_id')::bigint and parent.schema_name = $3 and \
         parent.end_snapshot is null and to_jsonb(parent)->>'parent_schema_id' is null))"
    )
}

/// Lists only tables owned by a destination's selected namespace.
///
/// Callers bind the optional namespace as `$1`.
pub fn namespaced_tables_query(metadata_schema: &str) -> String {
    let filter = table_namespace_filter(metadata_schema).replace("$3", "$1");
    let metadata_schema = quote_identifier(metadata_schema);
    format!(
        "select parent.schema_name, s.schema_name, t.table_name from \
         {metadata_schema}.ducklake_table t join {metadata_schema}.ducklake_schema s on \
         s.schema_id = t.schema_id left join {metadata_schema}.ducklake_schema parent on \
         parent.schema_id = (to_jsonb(s)->>'parent_schema_id')::bigint and parent.end_snapshot is \
         null where t.end_snapshot is null and s.end_snapshot is null and ({filter} or \
         (to_jsonb(s)->>'parent_schema_id' is null and s.schema_name = 'main' and \
         starts_with(t.table_name, '__etl_'))) order by s.schema_name, t.table_name"
    )
}

/// Checks nested-schema support without migrating an existing catalog.
pub fn namespace_version_check_sql(catalog: &str) -> String {
    let catalog = quote_identifier(catalog);
    format!(
        "select case when coalesce((select value from {catalog}.options() where option_name = \
         'version'), '') = '1.1' then true else error('DuckLake table_namespace requires catalog \
         format 1.1; recreate or explicitly migrate the catalog') end"
    )
}
