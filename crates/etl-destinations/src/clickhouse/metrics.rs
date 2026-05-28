use std::sync::Once;

use metrics::{Unit, describe_counter, describe_histogram};

static REGISTER_METRICS: Once = Once::new();

/// Duration of DDL operations sent to ClickHouse.
/// Labels: `kind` (`create_table`, `add_column`, `drop_column`,
/// `rename_column`, `truncate_table`, `drop_table`, `create_view`,
/// `drop_view`).
pub(super) const ETL_CLICKHOUSE_DDL_DURATION_SECONDS: &str = "etl_clickhouse_ddl_duration_seconds";

/// DDL failures sent to ClickHouse. Labels: `kind` (same set as
/// [`ETL_CLICKHOUSE_DDL_DURATION_SECONDS`]) and `outcome` (`timeout`,
/// `failed`).
pub(super) const ETL_CLICKHOUSE_DDL_ERRORS_TOTAL: &str = "etl_clickhouse_ddl_errors_total";

/// Duration of a single RowBinary INSERT statement from first write to server
/// acknowledgement. Labels: `source` (`copy` = initial table sync, `streaming`
/// = CDC events).
pub(super) const ETL_CLICKHOUSE_INSERT_DURATION_SECONDS: &str =
    "etl_clickhouse_insert_duration_seconds";

/// Rows committed in a single RowBinary INSERT statement.
/// Labels: `source` (`copy`, `streaming`).
pub(super) const ETL_CLICKHOUSE_INSERT_ROWS: &str = "etl_clickhouse_insert_rows";

/// Uncompressed RowBinary payload bytes per INSERT statement.
/// Labels: `source` (`copy`, `streaming`).
pub(super) const ETL_CLICKHOUSE_INSERT_BYTES: &str = "etl_clickhouse_insert_bytes";

/// INSERT statement failures. Labels: `source` (`copy`, `streaming`) and
/// `outcome` (`timeout`, `failed`).
pub(super) const ETL_CLICKHOUSE_INSERT_ERRORS_TOTAL: &str = "etl_clickhouse_insert_errors_total";

/// Number of INSERT statements committed for a single logical write batch.
/// Goes above 1 when [`ClickHouseInserterConfig::max_bytes_per_insert`] forces
/// a mid-batch flush. Labels: `source` (`copy`, `streaming`).
pub(super) const ETL_CLICKHOUSE_STATEMENTS_PER_BATCH: &str = "etl_clickhouse_statements_per_batch";

/// Duration of connectivity probes (`SELECT 1`, database-existence query).
pub(super) const ETL_CLICKHOUSE_CONNECTIVITY_CHECK_DURATION_SECONDS: &str =
    "etl_clickhouse_connectivity_check_duration_seconds";

/// Duration of schema lookups against `system.columns` / `system.tables`.
pub(super) const ETL_CLICKHOUSE_SCHEMA_QUERY_DURATION_SECONDS: &str =
    "etl_clickhouse_schema_query_duration_seconds";

/// Register ClickHouse-specific metrics.
///
/// Safe to call multiple times; registration happens only once.
pub(super) fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        describe_histogram!(
            ETL_CLICKHOUSE_DDL_DURATION_SECONDS,
            Unit::Seconds,
            "Duration of DDL operations sent to ClickHouse, labeled by kind"
        );

        describe_counter!(
            ETL_CLICKHOUSE_DDL_ERRORS_TOTAL,
            Unit::Count,
            "Total DDL failures sent to ClickHouse, labeled by kind and outcome"
        );

        describe_histogram!(
            ETL_CLICKHOUSE_INSERT_DURATION_SECONDS,
            Unit::Seconds,
            "Duration of RowBinary INSERT statements from first write to server acknowledgement, \
             labeled by source"
        );

        describe_histogram!(
            ETL_CLICKHOUSE_INSERT_ROWS,
            Unit::Count,
            "Rows committed per RowBinary INSERT statement, labeled by source"
        );

        describe_histogram!(
            ETL_CLICKHOUSE_INSERT_BYTES,
            Unit::Bytes,
            "Uncompressed RowBinary payload bytes per INSERT statement, labeled by source"
        );

        describe_counter!(
            ETL_CLICKHOUSE_INSERT_ERRORS_TOTAL,
            Unit::Count,
            "Total INSERT statement failures, labeled by source and outcome"
        );

        describe_histogram!(
            ETL_CLICKHOUSE_STATEMENTS_PER_BATCH,
            Unit::Count,
            "INSERT statements committed per logical write batch, labeled by source"
        );

        describe_histogram!(
            ETL_CLICKHOUSE_CONNECTIVITY_CHECK_DURATION_SECONDS,
            Unit::Seconds,
            "Duration of ClickHouse connectivity probes"
        );

        describe_histogram!(
            ETL_CLICKHOUSE_SCHEMA_QUERY_DURATION_SECONDS,
            Unit::Seconds,
            "Duration of ClickHouse schema lookups against system tables"
        );
    });
}
