use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};
use pg_escape::{quote_identifier, quote_literal};
use sqlx::PgPool;

/// Pending inline insert-data bytes sampled from the Postgres DuckLake catalog.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct DuckLakePendingInlineDataSizes {
    pub(super) inlined_data_bytes: u64,
}

/// Postgres-backed sampler for DuckLake inline insert-data table sizes.
#[derive(Clone)]
pub(super) struct DuckLakePendingInlineSizeSampler {
    metadata_schema: String,
    pool: PgPool,
}

impl DuckLakePendingInlineSizeSampler {
    /// Builds a sampler backed by the shared DuckLake metadata pool.
    pub(super) fn new(metadata_schema: String, pool: PgPool) -> Self {
        Self { metadata_schema, pool }
    }

    /// Samples pending inline insert-data bytes for one DuckLake table.
    pub(super) async fn sample_table(
        &self,
        table_name: &str,
    ) -> EtlResult<DuckLakePendingInlineDataSizes> {
        let sql = pending_inline_data_bytes_query(&self.metadata_schema);
        let inlined_data_bytes: i64 =
            sqlx::query_scalar(&sql).bind(table_name).fetch_one(&self.pool).await.map_err(
                |source| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake inline-size sampler query failed",
                        source: source
                    )
                },
            )?;

        Ok(DuckLakePendingInlineDataSizes { inlined_data_bytes: inlined_data_bytes.max(0) as u64 })
    }
}

/// Returns the PostgreSQL query that measures one table's inline insert-data
/// size.
fn pending_inline_data_bytes_query(metadata_schema: &str) -> String {
    let metadata_schema_literal = quote_literal(metadata_schema);
    let metadata_schema = quote_identifier(metadata_schema);
    let ducklake_table = quote_identifier("ducklake_table");
    let ducklake_inlined_data_tables = quote_identifier("ducklake_inlined_data_tables");

    format!(
        r#"WITH target_table AS (
             SELECT table_id
             FROM {metadata_schema}.{ducklake_table}
             WHERE end_snapshot IS NULL AND table_name = $1
             LIMIT 1
         ),
         target_inline_table AS (
             SELECT table_name
             FROM {metadata_schema}.{ducklake_inlined_data_tables}
             WHERE table_id = (SELECT table_id FROM target_table)
             LIMIT 1
         )
         SELECT COALESCE(
             pg_total_relation_size(
                 to_regclass(
                     format('%I.%I', {metadata_schema_literal}, (SELECT table_name FROM target_inline_table))
                 )
             ),
             0
         );"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_inline_data_bytes_query_qualifies_metadata_tables() {
        let sql = pending_inline_data_bytes_query("duck'lake");

        assert!(sql.contains("ducklake_table"));
        assert!(sql.contains("ducklake_inlined_data_tables"));
        assert!(sql.contains("duck'lake"));
        assert!(sql.contains(r#"format('%I.%I', 'duck''lake'"#));
    }
}
