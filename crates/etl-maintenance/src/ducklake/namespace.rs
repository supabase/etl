//! Namespace-aware metadata lookup shared by writers and maintenance.

use pg_escape::quote_identifier;

/// Builds a namespace filter without rewriting quoted metadata identifiers.
fn table_namespace_filter_for_parameter(metadata_schema: &str, parameter: usize) -> String {
    let metadata_schema = quote_identifier(metadata_schema);
    format!(
        "((${parameter}::text is null and to_jsonb(s)->>'parent_schema_id' is null) or exists \
         (select 1 from {metadata_schema}.ducklake_schema as parent where parent.schema_id = \
         (to_jsonb(s)->>'parent_schema_id')::bigint and parent.schema_name = ${parameter} and \
         parent.end_snapshot is null and to_jsonb(parent)->>'parent_schema_id' is null))"
    )
}

/// Filters a leaf schema `s` by the optional namespace bound to `$3`.
///
/// JSON extraction also works on format 1.0, where parent_schema_id is absent.
/// Parent identity is resolved by ID, never by splitting a dotted schema name.
pub fn table_namespace_filter(metadata_schema: &str) -> String {
    table_namespace_filter_for_parameter(metadata_schema, 3)
}

/// Lists only tables owned by a destination's selected namespace.
///
/// Callers bind the optional namespace as `$1`.
pub fn namespaced_tables_query(metadata_schema: &str) -> String {
    let filter = table_namespace_filter_for_parameter(metadata_schema, 1);
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

#[cfg(test)]
mod tests {
    use pg_escape::quote_identifier;

    use crate::ducklake::namespace::{namespaced_tables_query, table_namespace_filter};

    /// Discovery preserves parameter-like text in every metadata schema
    /// reference.
    #[test]
    fn namespaced_tables_query_preserves_metadata_schema_identifiers() {
        for metadata_schema in ["catalog$3", "catalog$30", "catalog\"$3"] {
            let quoted_schema = quote_identifier(metadata_schema);
            let query = namespaced_tables_query(metadata_schema);

            assert!(query.contains(&format!("from {quoted_schema}.ducklake_table t")));
            assert!(query.contains(&format!("from {quoted_schema}.ducklake_schema as parent")));
            assert!(query.contains("$1::text is null"));
            assert!(query.contains("parent.schema_name = $1"));
        }
    }

    /// Table lookups retain their third bind parameter and literal schema name.
    #[test]
    fn table_namespace_filter_preserves_metadata_schema_identifiers() {
        let filter = table_namespace_filter("catalog$3");

        assert!(filter.contains("from \"catalog$3\".ducklake_schema as parent"));
        assert!(filter.contains("$3::text is null"));
        assert!(filter.contains("parent.schema_name = $3"));
    }
}
