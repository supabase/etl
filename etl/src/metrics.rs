use std::sync::Once;

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

static REGISTER_METRICS: Once = Once::new();

pub const ETL_TABLES_TOTAL: &str = "etl_tables_total";
pub const ETL_BATCH_ITEMS_WRITTEN_TOTAL: &str = "etl_batch_items_written_total";
pub const ETL_TABLE_ROWS_TOTAL_WRITTEN: &str = "etl_table_rows_total_written";
pub const ETL_ITEMS_SEND_DURATION_SECONDS: &str = "etl_items_send_duration_seconds";
pub const ETL_TRANSACTION_DURATION_SECONDS: &str = "etl_transaction_duration_seconds";
pub const ETL_TRANSACTION_SIZE: &str = "etl_transaction_size";
pub const ETL_COPIED_TABLE_ROW_SIZE_BYTES: &str = "etl_copied_table_row_size_bytes";

/// Label key for replication phase (used by table state metrics).
pub const PHASE_LABEL: &str = "phase";
/// Label key for the ETL worker type ("table_sync" or "apply").
pub const WORKER_TYPE_LABEL: &str = "worker_type";
/// Label key for the action performed by the worker ("table_copy" or "table_streaming").
pub const ACTION_LABEL: &str = "action";
/// Label key used to tag metrics by destination implementation (e.g., "big_query").
pub const DESTINATION_LABEL: &str = "destination";
/// Label key for pipeline id.
pub const PIPELINE_ID_LABEL: &str = "pipeline_id";

/// Register metrics emitted by etl. This should be called before starting a pipeline.
/// It is safe to call this method multiple times. It is guaranteed to register the
/// metrics only once.
pub(crate) fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        describe_gauge!(
            ETL_TABLES_TOTAL,
            Unit::Count,
            "Total number of tables being copied"
        );

        describe_counter!(
            ETL_BATCH_ITEMS_WRITTEN_TOTAL,
            Unit::Count,
            "Total items written in batches, labeled by worker_type and action."
        );

        describe_gauge!(
            ETL_TABLE_ROWS_TOTAL_WRITTEN,
            Unit::Count,
            "Total number of table rows copied during table sync"
        );

        describe_histogram!(
            ETL_ITEMS_SEND_DURATION_SECONDS,
            Unit::Seconds,
            "Time taken in seconds to send batch items to the destination, labeled by worker_type and action"
        );

        describe_histogram!(
            ETL_TRANSACTION_DURATION_SECONDS,
            Unit::Seconds,
            "Duration in seconds between BEGIN and COMMIT for a transaction"
        );

        describe_histogram!(
            ETL_TRANSACTION_SIZE,
            Unit::Count,
            "Number of events contained in a single transaction"
        );

        describe_histogram!(
            ETL_COPIED_TABLE_ROW_SIZE_BYTES,
            Unit::Bytes,
            "Approximate size in bytes of a row copied during table sync"
        );
    });
}
