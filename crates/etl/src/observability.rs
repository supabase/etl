use std::sync::Once;

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

static REGISTER_METRICS: Once = Once::new();

pub(crate) const ETL_TABLES_TOTAL: &str = "etl_tables_total";
pub(crate) const ETL_BATCH_ITEMS_SEND_DURATION_SECONDS: &str =
    "etl_batch_items_send_duration_seconds";
pub(crate) const ETL_TRANSACTION_DURATION_SECONDS: &str = "etl_transaction_duration_seconds";
pub(crate) const ETL_TRANSACTIONS_TOTAL: &str = "etl_transactions_total";
pub(crate) const ETL_TRANSACTION_SIZE: &str = "etl_transaction_size";
pub(crate) const ETL_TABLE_COPY_DURATION_SECONDS: &str = "etl_table_copy_duration_seconds";
pub(crate) const ETL_TABLE_COPY_ROWS_TOTAL: &str = "etl_table_copy_rows_total";
pub(crate) const ETL_TABLE_COPY_PLANNED_PARTITIONS: &str = "etl_table_copy_planned_partitions";
pub(crate) const ETL_TABLE_COPY_EFFECTIVE_PARTITIONS: &str = "etl_table_copy_effective_partitions";
pub(crate) const ETL_TABLE_COPY_PARTITION_BLOCKS: &str = "etl_table_copy_partition_blocks";
pub(crate) const ETL_TABLE_COPY_PARTITION_ROWS: &str = "etl_table_copy_partition_rows";
pub(crate) const ETL_TABLE_COPY_PARTITION_DURATION_SECONDS: &str =
    "etl_table_copy_partition_duration_seconds";
pub(crate) const ETL_TABLE_COPY_PARTITIONS_TOTAL: &str = "etl_table_copy_partitions_total";
pub(crate) const ETL_TABLE_COPY_END_TO_END_LAG_BYTES: &str = "etl_table_copy_end_to_end_lag_bytes";
pub(crate) const ETL_BYTES_RECEIVED_TOTAL: &str = "etl_bytes_received_total";
pub(crate) const ETL_BYTES_PROCESSED_TOTAL: &str = "etl_bytes_processed_total";
pub(crate) const ETL_EVENTS_RECEIVED_TOTAL: &str = "etl_events_received_total";
pub(crate) const ETL_EVENTS_PROCESSED_TOTAL: &str = "etl_events_processed_total";
pub(crate) const ETL_REPLICATION_MESSAGES_TOTAL: &str = "etl_replication_messages_total";
pub(crate) const ETL_STATUS_UPDATES_TOTAL: &str = "etl_status_updates_total";
pub(crate) const ETL_STATUS_UPDATES_SKIPPED_TOTAL: &str = "etl_status_updates_skipped_total";
pub(crate) const ETL_SCHEMA_CLEANUPS_TOTAL: &str = "etl_schema_cleanups_total";
pub(crate) const ETL_SCHEMA_CLEANUP_ERRORS_TOTAL: &str = "etl_schema_cleanup_errors_total";
pub(crate) const ETL_SCHEMA_CLEANUP_TABLES_TOTAL: &str = "etl_schema_cleanup_tables_total";
pub(crate) const ETL_SCHEMA_CLEANUP_PRUNED_VERSIONS_TOTAL: &str =
    "etl_schema_cleanup_pruned_versions_total";
pub(crate) const ETL_DDL_SCHEMA_CHANGES_TOTAL: &str = "etl_ddl_schema_changes_total";
pub(crate) const ETL_DDL_SCHEMA_CHANGE_COLUMNS: &str = "etl_ddl_schema_change_columns";
pub(crate) const ETL_ROW_SIZE_BYTES: &str = "etl_row_size_bytes";
pub(crate) const ETL_SLOT_INVALIDATIONS_TOTAL: &str = "etl_slot_invalidations_total";
pub(crate) const ETL_WORKER_ERRORS_TOTAL: &str = "etl_worker_errors_total";
pub(crate) const ETL_MEMORY_BACKPRESSURE_ACTIVE: &str = "etl_memory_backpressure_active";
pub(crate) const ETL_MEMORY_BACKPRESSURE_TRANSITIONS_TOTAL: &str =
    "etl_memory_backpressure_transitions_total";
pub(crate) const ETL_MEMORY_BACKPRESSURE_ACTIVATION_DURATION_SECONDS: &str =
    "etl_memory_backpressure_activation_duration_seconds";
pub(crate) const ETL_IDEAL_BATCH_SIZE_BYTES: &str = "etl_ideal_batch_size_bytes";
pub(crate) const ETL_APPLY_LOOP_RECEIVED_LAG_BYTES: &str = "etl_apply_loop_received_lag_bytes";
pub(crate) const ETL_APPLY_LOOP_EFFECTIVE_FLUSH_LAG_BYTES: &str =
    "etl_apply_loop_effective_flush_lag_bytes";
pub(crate) const ETL_APPLY_LOOP_FLUSH_LAG_BYTES: &str = "etl_apply_loop_flush_lag_bytes";
pub(crate) const ETL_APPLY_LOOP_END_TO_END_LAG_BYTES: &str = "etl_apply_loop_end_to_end_lag_bytes";

/// Label key for table state (used by table state metrics).
pub(crate) const STATE_LABEL: &str = "state";
/// Label key for the ETL worker type ("table_sync" or "apply").
pub(crate) const WORKER_TYPE_LABEL: &str = "worker_type";
/// Label key for the action performed by the worker ("table_copy" or
/// "table_streaming").
pub(crate) const ACTION_LABEL: &str = "action";
/// Label key for event type (copy, insert, update, delete).
pub(crate) const EVENT_TYPE_LABEL: &str = "event_type";
/// Label key for whether the status update was forced.
pub(crate) const FORCED_LABEL: &str = "forced";
/// Label key for the status update type.
pub(crate) const STATUS_UPDATE_TYPE_LABEL: &str = "status_update_type";
/// Label key for the DDL command tag emitted by Postgres.
pub(crate) const COMMAND_TAG_LABEL: &str = "command_tag";
/// Label key for the outcome of an operation.
pub(crate) const OUTCOME_LABEL: &str = "outcome";
/// Label key for worker error classification ("timed", "manual", "no_retry").
pub(crate) const ERROR_TYPE_LABEL: &str = "error_type";
/// Label key for transition direction ("activate" or "resume").
pub(crate) const DIRECTION_LABEL: &str = "direction";

/// Register metrics emitted by etl. This should be called before starting a
/// pipeline. It is safe to call this method multiple times. It is guaranteed to
/// register the metrics only once.
pub(crate) fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        describe_gauge!(ETL_TABLES_TOTAL, Unit::Count, "Total number of tables being copied");

        describe_histogram!(
            ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
            Unit::Seconds,
            "Time taken in seconds to send a batch of items to the destination, labeled by \
             worker_type and action"
        );

        describe_histogram!(
            ETL_TRANSACTION_DURATION_SECONDS,
            Unit::Seconds,
            "Duration in seconds between BEGIN and COMMIT for a transaction"
        );

        describe_counter!(
            ETL_TRANSACTIONS_TOTAL,
            Unit::Count,
            "Total number of transactions seen."
        );

        describe_histogram!(ETL_TRANSACTION_SIZE, Unit::Count, "Number of events per transaction.");

        describe_histogram!(
            ETL_TABLE_COPY_DURATION_SECONDS,
            Unit::Seconds,
            "Duration in seconds to complete one initial table copy."
        );

        describe_counter!(
            ETL_TABLE_COPY_ROWS_TOTAL,
            Unit::Count,
            "Total rows flushed by completed table copies."
        );

        describe_histogram!(
            ETL_TABLE_COPY_PLANNED_PARTITIONS,
            Unit::Count,
            "Target CTID partitions planned for one table copy before per-physical-table \
             block-count clamping."
        );

        describe_histogram!(
            ETL_TABLE_COPY_EFFECTIVE_PARTITIONS,
            Unit::Count,
            "Actual CTID partitions used for one table copy after empty physical-table filtering \
             and block-count clamping."
        );

        describe_histogram!(
            ETL_TABLE_COPY_PARTITION_BLOCKS,
            Unit::Count,
            "Estimated heap blocks assigned to one table copy partition."
        );

        describe_histogram!(
            ETL_TABLE_COPY_PARTITION_ROWS,
            Unit::Count,
            "Rows flushed by one completed table copy partition."
        );

        describe_histogram!(
            ETL_TABLE_COPY_PARTITION_DURATION_SECONDS,
            Unit::Seconds,
            "Duration in seconds to copy one completed table copy partition."
        );

        describe_counter!(
            ETL_TABLE_COPY_PARTITIONS_TOTAL,
            Unit::Count,
            "Total completed table copy partitions."
        );

        describe_gauge!(
            ETL_TABLE_COPY_END_TO_END_LAG_BYTES,
            Unit::Bytes,
            "Current WAL bytes between the source current LSN and the table copy slot consistent \
             point."
        );

        describe_counter!(
            ETL_EVENTS_PROCESSED_TOTAL,
            Unit::Count,
            "Total number of events successfully processed, labeled by worker_type and action."
        );

        describe_counter!(
            ETL_EVENTS_RECEIVED_TOTAL,
            Unit::Count,
            "Total number of events received from the source, labeled by worker_type and action."
        );

        describe_counter!(
            ETL_REPLICATION_MESSAGES_TOTAL,
            Unit::Count,
            "Total number of logical replication messages received by the apply loop, labeled by \
             worker_type."
        );

        describe_counter!(
            ETL_BYTES_PROCESSED_TOTAL,
            Unit::Bytes,
            "Total PostgreSQL COPY row-body or pgoutput tuple-value bytes acknowledged by \
             successful destination writes, labeled by event_type."
        );

        describe_counter!(
            ETL_BYTES_RECEIVED_TOTAL,
            Unit::Bytes,
            "Total PostgreSQL COPY row-body or pgoutput tuple-value bytes received from the \
             source, labeled by event_type."
        );

        describe_counter!(
            ETL_STATUS_UPDATES_TOTAL,
            Unit::Count,
            "Total number of status updates sent to Postgres, labeled by forced and \
             status_update_type."
        );

        describe_counter!(
            ETL_STATUS_UPDATES_SKIPPED_TOTAL,
            Unit::Count,
            "Total number of status updates skipped due to throttling, labeled by \
             status_update_type."
        );

        describe_counter!(
            ETL_SCHEMA_CLEANUPS_TOTAL,
            Unit::Count,
            "Total number of asynchronous schema cleanup tasks completed, labeled by worker_type."
        );

        describe_counter!(
            ETL_SCHEMA_CLEANUP_ERRORS_TOTAL,
            Unit::Count,
            "Total number of asynchronous schema cleanup errors, labeled by worker_type."
        );

        describe_counter!(
            ETL_SCHEMA_CLEANUP_TABLES_TOTAL,
            Unit::Count,
            "Total number of tables considered by asynchronous schema cleanup tasks, labeled by \
             worker_type."
        );

        describe_counter!(
            ETL_SCHEMA_CLEANUP_PRUNED_VERSIONS_TOTAL,
            Unit::Count,
            "Total number of obsolete schema versions pruned by asynchronous schema cleanup \
             tasks, labeled by worker_type."
        );

        describe_counter!(
            ETL_DDL_SCHEMA_CHANGES_TOTAL,
            Unit::Count,
            "Total number of DDL schema change messages handled, labeled by worker_type, \
             command_tag, and outcome."
        );

        describe_histogram!(
            ETL_DDL_SCHEMA_CHANGE_COLUMNS,
            Unit::Count,
            "Number of columns in applied DDL schema change snapshots, labeled by worker_type and \
             command_tag."
        );

        describe_histogram!(
            ETL_ROW_SIZE_BYTES,
            Unit::Bytes,
            "Distribution of individual PostgreSQL COPY row-body or pgoutput row-event \
             tuple-value payload sizes in bytes, labeled by event_type."
        );

        describe_counter!(
            ETL_SLOT_INVALIDATIONS_TOTAL,
            Unit::Count,
            "Total number of times a replication slot was found invalidated on pipeline start."
        );

        describe_counter!(
            ETL_WORKER_ERRORS_TOTAL,
            Unit::Count,
            "Total number of worker errors, labeled by worker_type and error_type."
        );

        describe_gauge!(
            ETL_MEMORY_BACKPRESSURE_ACTIVE,
            Unit::Count,
            "Memory backpressure current state (0 or 1)."
        );

        describe_counter!(
            ETL_MEMORY_BACKPRESSURE_TRANSITIONS_TOTAL,
            Unit::Count,
            "Total memory backpressure state transitions, labeled by direction."
        );

        describe_histogram!(
            ETL_MEMORY_BACKPRESSURE_ACTIVATION_DURATION_SECONDS,
            Unit::Seconds,
            "Duration in seconds of each memory backpressure active period."
        );

        describe_gauge!(
            ETL_IDEAL_BATCH_SIZE_BYTES,
            Unit::Bytes,
            "Current ideal batch size in bytes."
        );

        describe_gauge!(
            ETL_APPLY_LOOP_RECEIVED_LAG_BYTES,
            Unit::Bytes,
            "Difference between the source Postgres current WAL position and the last LSN \
             received by ETL."
        );

        describe_gauge!(
            ETL_APPLY_LOOP_EFFECTIVE_FLUSH_LAG_BYTES,
            Unit::Bytes,
            "Difference between ETL's last received LSN and effective flush LSN. The effective \
             flush LSN is the apply-loop progress frontier; when the loop is idle and no data \
             needs flushing, it follows the last received LSN."
        );

        describe_gauge!(
            ETL_APPLY_LOOP_FLUSH_LAG_BYTES,
            Unit::Bytes,
            "Difference between ETL's last received LSN and last flush LSN. The last flush LSN is \
             the latest LSN whose data was durably flushed to the destination; idle-only progress \
             is not included."
        );

        describe_gauge!(
            ETL_APPLY_LOOP_END_TO_END_LAG_BYTES,
            Unit::Bytes,
            "Difference between the source Postgres current WAL position and ETL's effective \
             flush LSN."
        );
    });
}
