use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};
use metrics::gauge;
use pg_escape::{quote_identifier, quote_literal};
use sqlx::PgPool;

use crate::ducklake::metrics::{ETL_DUCKLAKE_TABLE_ACTIVE_INLINED_DATA_BYTES, TABLE_LABEL};

/// Pending inlined bytes sampled from the Postgres DuckLake catalog.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct DuckLakePendingInlineDataSizes {
    pub(super) inlined_bytes: u64,
}

/// Postgres-backed sampler for DuckLake inlined catalog table sizes.
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

    /// Samples pending inlined bytes for one DuckLake table.
    pub(super) async fn sample_table(
        &self,
        table_name: &str,
    ) -> EtlResult<DuckLakePendingInlineDataSizes> {
        let sql = pending_inline_data_bytes_query(&self.metadata_schema);
        let inlined_bytes: i64 =
            sqlx::query_scalar(&sql).bind(table_name).fetch_one(&self.pool).await.map_err(
                |source| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake inline-size sampler query failed",
                        source: source
                    )
                },
            )?;
        Ok(record_pending_inline_data_sizes(inlined_bytes, table_name))
    }
}

/// Records sampled pending inlined bytes for one table.
fn record_pending_inline_data_sizes(
    inlined_bytes: i64,
    table_name: &str,
) -> DuckLakePendingInlineDataSizes {
    let inlined_bytes = inlined_bytes.max(0) as u64;
    gauge!(ETL_DUCKLAKE_TABLE_ACTIVE_INLINED_DATA_BYTES, TABLE_LABEL => table_name.to_string())
        .set(inlined_bytes as f64);
    DuckLakePendingInlineDataSizes { inlined_bytes }
}

/// Returns the PostgreSQL query that measures one table's pending inlined size.
fn pending_inline_data_bytes_query(metadata_schema: &str) -> String {
    let metadata_schema_literal = quote_literal(metadata_schema);
    let metadata_schema = quote_identifier(metadata_schema);
    let ducklake_table = quote_identifier("ducklake_table");
    let ducklake_inlined_data_tables = quote_identifier("ducklake_inlined_data_tables");

    // DuckLake uses one inlined insert table per (table_id, schema_version),
    // but one inlined delete table per table_id. We therefore sum every
    // registered inlined data table for the current table and add the single
    // deterministic delete-table relation size.
    format!(
        r"WITH target_table AS (
             SELECT table_id
             FROM {metadata_schema}.{ducklake_table}
             WHERE end_snapshot IS NULL AND table_name = $1
             LIMIT 1
         ),
         target_inline_tables AS (
             SELECT DISTINCT table_name
             FROM {metadata_schema}.{ducklake_inlined_data_tables}
             WHERE table_id = (SELECT table_id FROM target_table)
         ),
         inline_data_bytes AS (
             SELECT COALESCE(
                 SUM(
                     pg_total_relation_size(
                         to_regclass(
                             format('%I.%I', {metadata_schema_literal}, table_name)
                         )
                     )
                 ),
                 0
             )::BIGINT AS total_bytes
             FROM target_inline_tables
         ),
         inline_delete_bytes AS (
             SELECT COALESCE(
                 pg_total_relation_size(
                     to_regclass(
                         format(
                             '%I.%I',
                             {metadata_schema_literal},
                             format('ducklake_inlined_delete_%s', (SELECT table_id FROM target_table))
                         )
                     )
                 ),
                 0
             )::BIGINT AS total_bytes
         )
         SELECT
             (SELECT total_bytes FROM inline_data_bytes)
             + (SELECT total_bytes FROM inline_delete_bytes);"
    )
}

#[cfg(test)]
mod tests {
    use etl_telemetry::metrics::init_metrics_handle;

    use super::*;
    use crate::ducklake::metrics::register_metrics;

    fn active_inlined_data_bytes_gauge_value(rendered: &str) -> f64 {
        rendered
            .lines()
            .find_map(|line| {
                if line.starts_with(ETL_DUCKLAKE_TABLE_ACTIVE_INLINED_DATA_BYTES) {
                    line.split_whitespace().last()?.parse::<f64>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0.0)
    }

    #[test]
    fn pending_inline_data_bytes_query_qualifies_metadata_tables() {
        let sql = pending_inline_data_bytes_query("duck'lake");

        assert!(sql.contains("ducklake_table"));
        assert!(sql.contains("ducklake_inlined_data_tables"));
        assert!(sql.contains("ducklake_inlined_delete_%s"));
        assert!(sql.contains("duck'lake"));
        assert!(sql.contains(r#"format('%I.%I', 'duck''lake'"#));
        assert!(sql.contains("SUM("));
        assert!(sql.contains(")::BIGINT AS total_bytes"));
    }

    #[tokio::test]
    async fn recording_pending_inline_data_sizes_exports_gauge_value() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();

        let rendered_before = handle.render();
        let value_before = active_inlined_data_bytes_gauge_value(&rendered_before);

        let sizes = record_pending_inline_data_sizes(1_024, "test");

        let rendered_after = handle.render();
        let value_after = active_inlined_data_bytes_gauge_value(&rendered_after);

        assert_eq!(sizes, DuckLakePendingInlineDataSizes { inlined_bytes: 1_024 });
        assert!(
            value_after >= value_before.max(1_024.0),
            "active inlined data gauge did not reflect the recorded sample"
        );
        assert_eq!(value_after, 1_024.0);
    }
}
