//! PostgreSQL publication naming and membership queries.

use pg_escape::quote_literal;

/// Prefix used for publications created by Supabase ETL.
pub const ETL_PUBLICATION_PREFIX: &str = "supabase_etl_publication";

/// Builds the table-ID query shared by replication startup and API preflight.
///
/// PostgreSQL expands explicit tables, schema publications, and all-table
/// publications according to `publish_via_partition_root`. Deduplication
/// returns each logical table once. The publication name is escaped as a SQL
/// literal so this query also works on replication connections using the simple
/// protocol.
pub fn publication_table_ids_query(publication_name: &str) -> String {
    format!(
        r#"
        select distinct gpt.relid::oid as oid
        from pg_catalog.pg_get_publication_tables({}) gpt
        order by oid
        "#,
        quote_literal(publication_name)
    )
}

/// Returns the deterministic ETL publication name for a pipeline.
pub fn etl_publication_name(pipeline_id: i64) -> String {
    format!("{ETL_PUBLICATION_PREFIX}_{pipeline_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_name_contains_pipeline_id() {
        assert_eq!(etl_publication_name(42), "supabase_etl_publication_42");
        assert!(etl_publication_name(i64::MAX).len() <= 63);
    }
}
