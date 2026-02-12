use std::sync::Once;

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

static REGISTER_METRICS: Once = Once::new();

pub const ETL_TABLES_TOTAL: &str = "etl_tables_total";
pub const ETL_BATCH_ITEMS_SEND_DURATION_SECONDS: &str = "etl_batch_items_send_duration_seconds";
pub const ETL_TRANSACTION_DURATION_SECONDS: &str = "etl_transaction_duration_seconds";
pub const ETL_TRANSACTIONS_TOTAL: &str = "etl_transactions_total";
pub const ETL_TRANSACTION_SIZE: &str = "etl_transaction_size";
pub const ETL_TABLE_COPY_DURATION_SECONDS: &str = "etl_table_copy_duration_seconds";
pub const ETL_TABLE_COPY_ROWS: &str = "etl_table_copy_rows";
pub const ETL_PARALLEL_TABLE_COPY_TIME_IMBALANCE: &str = "etl_parallel_table_copy_time_imbalance";
pub const ETL_PARALLEL_TABLE_COPY_ROWS_IMBALANCE: &str = "etl_parallel_table_copy_rows_imbalance";
pub const ETL_BYTES_PROCESSED_TOTAL: &str = "etl_bytes_processed_total";
pub const ETL_EVENTS_PROCESSED_TOTAL: &str = "etl_events_processed_total";
pub const ETL_STATUS_UPDATES_TOTAL: &str = "etl_status_updates_total";
pub const ETL_STATUS_UPDATES_SKIPPED_TOTAL: &str = "etl_status_updates_skipped_total";
pub const ETL_ROW_SIZE_BYTES: &str = "etl_row_size_bytes";
pub const ETL_SLOT_INVALIDATIONS_TOTAL: &str = "etl_slot_invalidations_total";
pub const ETL_WORKER_ERRORS_TOTAL: &str = "etl_worker_errors_total";

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
/// Label to tag the table copy metric if it was using partitioning.
pub const PARTITIONING_LABEL: &str = "partitioning";
/// Label key for event type (copy, insert, update, delete).
pub const EVENT_TYPE_LABEL: &str = "event_type";
/// Label key for whether the status update was forced.
pub const FORCED_LABEL: &str = "forced";
/// Label key for the status update type.
pub const STATUS_UPDATE_TYPE_LABEL: &str = "status_update_type";
/// Label key for worker error classification ("timed", "manual", "no_retry").
pub const ERROR_TYPE_LABEL: &str = "error_type";

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

        describe_counter!(
            ETL_TRANSACTIONS_TOTAL,
            Unit::Count,
            "Total number of transactions seen, labeled by pipeline_id"
        );

        describe_histogram!(
            ETL_TRANSACTION_SIZE,
            Unit::Count,
            "Number of events per transaction, labeled by pipeline_id"
        );

        describe_histogram!(
            ETL_TABLE_COPY_DURATION_SECONDS,
            Unit::Seconds,
            "Duration in seconds to complete initial table copy from DataSync to FinishedCopy phase"
        );

        describe_histogram!(
            ETL_TABLE_COPY_ROWS,
            Unit::Count,
            "Number of rows copied per table copy partition, labeled by pipeline_id, destination, and partitioning"
        );

        describe_histogram!(
            ETL_PARALLEL_TABLE_COPY_TIME_IMBALANCE,
            "Load Imbalance Factor for parallel table copy duration (max_time / avg_time), labeled by pipeline_id and destination. Value of 1.0 indicates perfect balance, higher values indicate more imbalance."
        );

        describe_histogram!(
            ETL_PARALLEL_TABLE_COPY_ROWS_IMBALANCE,
            "Load Imbalance Factor for parallel table copy row distribution (max_rows / avg_rows), labeled by pipeline_id and destination. Value of 1.0 indicates perfect balance, higher values indicate more imbalance."
        );

        describe_counter!(
            ETL_EVENTS_PROCESSED_TOTAL,
            Unit::Count,
            "Total number of events successfully processed (stored), labeled by worker_type, action, pipeline_id, and destination"
        );

        describe_counter!(
            ETL_BYTES_PROCESSED_TOTAL,
            Unit::Bytes,
            "Total bytes processed by the pipeline, labeled by pipeline_id and event_type"
        );

        describe_counter!(
            ETL_STATUS_UPDATES_TOTAL,
            Unit::Count,
            "Total number of status updates sent to Postgres, labeled by pipeline_id and forced"
        );

        describe_counter!(
            ETL_STATUS_UPDATES_SKIPPED_TOTAL,
            Unit::Count,
            "Total number of status updates skipped due to throttling, labeled by pipeline_id"
        );

        describe_histogram!(
            ETL_ROW_SIZE_BYTES,
            Unit::Bytes,
            "Distribution of individual row sizes in bytes, labeled by event_type"
        );

        describe_counter!(
            ETL_SLOT_INVALIDATIONS_TOTAL,
            Unit::Count,
            "Total number of times a replication slot was found invalidated on pipeline start, labeled by pipeline_id"
        );

        describe_counter!(
            ETL_WORKER_ERRORS_TOTAL,
            Unit::Count,
            "Total number of worker errors, labeled by pipeline_id, worker_type, and error_type"
        );
    });
}
