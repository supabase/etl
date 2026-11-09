use etl::error::{ErrorKind, EtlResult};
use etl_maintenance::ducklake::pending_inline_data_bytes_query;
use metrics::gauge;
use sqlx::{AssertSqlSafe, PgPool};

use crate::{
    ducklake::{
        DuckLakeTableName,
        metrics::{ETL_DUCKLAKE_TABLE_ACTIVE_INLINED_DATA_BYTES, TABLE_LABEL},
    },
    retry::classified_error,
};

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
        table_name: &DuckLakeTableName,
    ) -> EtlResult<DuckLakePendingInlineDataSizes> {
        let sql = pending_inline_data_bytes_query(&self.metadata_schema);
        let inlined_bytes: i64 = async {
            let mut transaction = self.pool.begin().await?;
            let result = sqlx::query_scalar(AssertSqlSafe(sql))
                .persistent(false)
                .bind(table_name.schema())
                .bind(table_name.table())
                .bind(table_name.namespace())
                .fetch_one(&mut *transaction)
                .await?;
            transaction.commit().await?;
            Ok::<_, sqlx::Error>(result)
        }
        .await
        .map_err(|source| {
            classified_error(
                ErrorKind::DestinationQueryFailed,
                "DuckLake inline-size sampler query failed",
                source,
            )
        })?;
        Ok(record_pending_inline_data_sizes(inlined_bytes, &table_name.id()))
    }
}

/// Records sampled pending inlined bytes for one table.
fn record_pending_inline_data_sizes(
    inlined_bytes: i64,
    table_name: &str,
) -> DuckLakePendingInlineDataSizes {
    let inlined_bytes = inlined_bytes.max(0) as u64;
    gauge!(ETL_DUCKLAKE_TABLE_ACTIVE_INLINED_DATA_BYTES, TABLE_LABEL => table_name.to_owned())
        .set(inlined_bytes as f64);
    DuckLakePendingInlineDataSizes { inlined_bytes }
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
