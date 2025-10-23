use std::sync::Once;

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

static REGISTER_METRICS: Once = Once::new();

pub const ETL_TABLES_TOTAL: &str = "etl_tables_total";
pub const ETL_BATCH_ITEMS_SEND_DURATION_SECONDS: &str = "etl_batch_items_send_duration_seconds";
pub const ETL_TRANSACTION_DURATION_SECONDS: &str = "etl_transaction_duration_seconds";
pub const ETL_TRANSACTION_SIZE: &str = "etl_transaction_size";
pub const ETL_COPIED_TABLE_ROW_SIZE_BYTES: &str = "etl_copied_table_row_size_bytes";
pub const ETL_TABLE_COPY_DURATION_SECONDS: &str = "etl_table_copy_duration_seconds";
pub const ETL_PIPELINE_ERRORS_TOTAL: &str = "etl_pipeline_errors_total";
pub const ETL_EVENTS_RECEIVED_TOTAL: &str = "etl_events_received_total";
pub const ETL_EVENTS_PROCESSED_TOTAL: &str = "etl_events_processed_total";

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
/// Label key for whether the error was handled (caught) or unhandled (bubbled up).
pub const ERROR_HANDLED_LABEL: &str = "handled";

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

        describe_histogram!(
            ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
            Unit::Seconds,
            "Time taken in seconds to send a batch of items to the destination, labeled by worker_type and action"
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

        describe_histogram!(
            ETL_TABLE_COPY_DURATION_SECONDS,
            Unit::Seconds,
            "Duration in seconds to complete initial table copy from DataSync to FinishedCopy phase"
        );

        describe_counter!(
            ETL_PIPELINE_ERRORS_TOTAL,
            Unit::Count,
            "Total number of errors in the pipeline, labeled by error_kind and handled status"
        );

        describe_counter!(
            ETL_EVENTS_RECEIVED_TOTAL,
            Unit::Count,
            "Total number of events received, labeled by worker_type, action, and pipeline_id"
        );

        describe_counter!(
            ETL_EVENTS_PROCESSED_TOTAL,
            Unit::Count,
            "Total number of events successfully processed (stored), labeled by worker_type, action, pipeline_id, and destination"
        );
    });
}
