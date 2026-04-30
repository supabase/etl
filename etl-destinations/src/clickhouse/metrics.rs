use std::sync::Once;

use metrics::{Unit, describe_histogram};

static REGISTER_METRICS: Once = Once::new();

/// Duration of `CREATE TABLE IF NOT EXISTS` DDL operations sent to ClickHouse.
/// Labels: `table`.
pub(super) const ETL_CLICKHOUSE_DDL_DURATION_SECONDS: &str = "etl_clickhouse_ddl_duration_seconds";

/// Duration of a single RowBinary INSERT statement from first write to server
/// acknowledgement. Labels: `table`, `source` (`copy` = initial table sync,
/// `streaming` = CDC events).
pub(super) const ETL_CLICKHOUSE_INSERT_DURATION_SECONDS: &str =
    "etl_clickhouse_insert_duration_seconds";

/// Register ClickHouse-specific metrics.
///
/// Safe to call multiple times — registration happens only once.
pub(super) fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        describe_histogram!(
            ETL_CLICKHOUSE_DDL_DURATION_SECONDS,
            Unit::Seconds,
            "Duration of CREATE TABLE IF NOT EXISTS DDL operations sent to ClickHouse, labeled by \
             table"
        );

        describe_histogram!(
            ETL_CLICKHOUSE_INSERT_DURATION_SECONDS,
            Unit::Seconds,
            "Duration of RowBinary INSERT statements from first write to server acknowledgement, \
             labeled by table and source"
        );
    });
}
