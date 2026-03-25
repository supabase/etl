use std::sync::Once;

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

static REGISTER_METRICS: Once = Once::new();

pub(crate) const ETL_DUCKLAKE_POOL_SIZE: &str = "etl_ducklake_pool_size";
pub(crate) const ETL_DUCKLAKE_BLOCKING_SLOT_WAIT_SECONDS: &str =
    "etl_ducklake_blocking_slot_wait_seconds";
pub(crate) const ETL_DUCKLAKE_POOL_CHECKOUT_WAIT_SECONDS: &str =
    "etl_ducklake_pool_checkout_wait_seconds";
pub(crate) const ETL_DUCKLAKE_BLOCKING_OPERATION_DURATION_SECONDS: &str =
    "etl_ducklake_blocking_operation_duration_seconds";
pub(crate) const ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS: &str =
    "etl_ducklake_batch_commit_duration_seconds";
pub(crate) const ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS: &str =
    "etl_ducklake_batch_prepared_mutations";
pub(crate) const ETL_DUCKLAKE_UPSERT_ROWS: &str = "etl_ducklake_upsert_rows";
pub(crate) const ETL_DUCKLAKE_DELETE_PREDICATES: &str = "etl_ducklake_delete_predicates";
pub(crate) const ETL_DUCKLAKE_INLINE_FLUSH_ROWS: &str = "etl_ducklake_inline_flush_rows";
pub(crate) const ETL_DUCKLAKE_INLINE_FLUSH_DURATION_SECONDS: &str =
    "etl_ducklake_inline_flush_duration_seconds";
pub(crate) const ETL_DUCKLAKE_RETRIES_TOTAL: &str = "etl_ducklake_retries_total";
pub(crate) const ETL_DUCKLAKE_FAILED_BATCHES_TOTAL: &str = "etl_ducklake_failed_batches_total";
pub(crate) const ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL: &str = "etl_ducklake_replayed_batches_total";
pub(crate) const ETL_DUCKLAKE_TABLE_ACTIVE_DATA_FILES: &str =
    "etl_ducklake_table_active_data_files";
pub(crate) const ETL_DUCKLAKE_TABLE_ACTIVE_DATA_BYTES: &str =
    "etl_ducklake_table_active_data_bytes";
pub(crate) const ETL_DUCKLAKE_TABLE_ACTIVE_DATA_FILE_AVG_SIZE_BYTES: &str =
    "etl_ducklake_table_active_data_file_avg_size_bytes";
pub(crate) const ETL_DUCKLAKE_TABLE_SMALL_FILE_RATIO: &str = "etl_ducklake_table_small_file_ratio";
pub(crate) const ETL_DUCKLAKE_TABLE_ACTIVE_DELETE_FILES: &str =
    "etl_ducklake_table_active_delete_files";
pub(crate) const ETL_DUCKLAKE_TABLE_ACTIVE_DELETE_BYTES: &str =
    "etl_ducklake_table_active_delete_bytes";
pub(crate) const ETL_DUCKLAKE_TABLE_DELETED_ROW_RATIO: &str =
    "etl_ducklake_table_deleted_row_ratio";
pub(crate) const ETL_DUCKLAKE_SNAPSHOTS_TOTAL: &str = "etl_ducklake_snapshots_total";
pub(crate) const ETL_DUCKLAKE_OLDEST_SNAPSHOT_AGE_SECONDS: &str =
    "etl_ducklake_oldest_snapshot_age_seconds";
pub(crate) const ETL_DUCKLAKE_FILES_SCHEDULED_FOR_DELETION_TOTAL: &str =
    "etl_ducklake_files_scheduled_for_deletion_total";
pub(crate) const ETL_DUCKLAKE_FILES_SCHEDULED_FOR_DELETION_BYTES: &str =
    "etl_ducklake_files_scheduled_for_deletion_bytes";
pub(crate) const ETL_DUCKLAKE_OLDEST_SCHEDULED_DELETION_AGE_SECONDS: &str =
    "etl_ducklake_oldest_scheduled_deletion_age_seconds";

pub(crate) const BATCH_KIND_LABEL: &str = "batch_kind";
pub(crate) const SUB_BATCH_KIND_LABEL: &str = "sub_batch_kind";
pub(crate) const PREPARED_ROWS_KIND_LABEL: &str = "prepared_rows_kind";
pub(crate) const DELETE_ORIGIN_LABEL: &str = "delete_origin";
pub(crate) const RETRY_SCOPE_LABEL: &str = "retry_scope";
pub(crate) const RESULT_LABEL: &str = "result";

/// Registers DuckLake metrics once per process.
pub(crate) fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        describe_gauge!(
            ETL_DUCKLAKE_POOL_SIZE,
            Unit::Count,
            "Configured size of the warm DuckLake connection pool."
        );

        describe_histogram!(
            ETL_DUCKLAKE_BLOCKING_SLOT_WAIT_SECONDS,
            Unit::Seconds,
            "Time spent waiting for a DuckLake blocking slot."
        );
        describe_histogram!(
            ETL_DUCKLAKE_POOL_CHECKOUT_WAIT_SECONDS,
            Unit::Seconds,
            "Time spent waiting to check out a DuckLake connection from the pool."
        );
        describe_histogram!(
            ETL_DUCKLAKE_BLOCKING_OPERATION_DURATION_SECONDS,
            Unit::Seconds,
            "Time spent executing a DuckLake blocking operation after checkout."
        );
        describe_histogram!(
            ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS,
            Unit::Seconds,
            "End-to-end duration of a committed DuckLake atomic batch, labeled by batch_kind and sub_batch_kind."
        );
        describe_histogram!(
            ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS,
            Unit::Count,
            "Prepared mutation statements per committed DuckLake atomic batch, labeled by batch_kind and sub_batch_kind."
        );
        describe_histogram!(
            ETL_DUCKLAKE_UPSERT_ROWS,
            Unit::Count,
            "Rows included in one DuckLake upsert statement, labeled by batch_kind and prepared_rows_kind."
        );
        describe_histogram!(
            ETL_DUCKLAKE_DELETE_PREDICATES,
            Unit::Count,
            "Predicates included in one DuckLake delete statement, labeled by batch_kind and delete_origin."
        );
        describe_histogram!(
            ETL_DUCKLAKE_INLINE_FLUSH_ROWS,
            Unit::Count,
            "Rows materialized by ducklake_flush_inlined_data, labeled by batch_kind and result."
        );
        describe_histogram!(
            ETL_DUCKLAKE_INLINE_FLUSH_DURATION_SECONDS,
            Unit::Seconds,
            "Duration of ducklake_flush_inlined_data, labeled by batch_kind and result."
        );
        describe_counter!(
            ETL_DUCKLAKE_RETRIES_TOTAL,
            Unit::Count,
            "Retry attempts for DuckLake write paths, labeled by batch_kind and retry_scope."
        );
        describe_counter!(
            ETL_DUCKLAKE_FAILED_BATCHES_TOTAL,
            Unit::Count,
            "DuckLake batch executions that still failed after retries, labeled by batch_kind and retry_scope."
        );
        describe_counter!(
            ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL,
            Unit::Count,
            "DuckLake batches skipped because an applied marker already existed, labeled by batch_kind."
        );

        describe_histogram!(
            ETL_DUCKLAKE_TABLE_ACTIVE_DATA_FILES,
            Unit::Count,
            "Sampled active data-file count for one DuckLake table from the background metrics task."
        );
        describe_histogram!(
            ETL_DUCKLAKE_TABLE_ACTIVE_DATA_BYTES,
            Unit::Bytes,
            "Sampled active data bytes for one DuckLake table from the background metrics task."
        );
        describe_histogram!(
            ETL_DUCKLAKE_TABLE_ACTIVE_DATA_FILE_AVG_SIZE_BYTES,
            Unit::Bytes,
            "Sampled average active data-file size for one DuckLake table from the background metrics task."
        );
        describe_histogram!(
            ETL_DUCKLAKE_TABLE_SMALL_FILE_RATIO,
            "Sampled ratio of active data files smaller than 5 MiB for one DuckLake table from the background metrics task."
        );
        describe_histogram!(
            ETL_DUCKLAKE_TABLE_ACTIVE_DELETE_FILES,
            Unit::Count,
            "Sampled active delete-file count for one DuckLake table from the background metrics task."
        );
        describe_histogram!(
            ETL_DUCKLAKE_TABLE_ACTIVE_DELETE_BYTES,
            Unit::Bytes,
            "Sampled active delete-file bytes for one DuckLake table from the background metrics task."
        );
        describe_histogram!(
            ETL_DUCKLAKE_TABLE_DELETED_ROW_RATIO,
            "Sampled ratio of active deleted rows to active data-file rows for one DuckLake table from the background metrics task."
        );

        describe_gauge!(
            ETL_DUCKLAKE_SNAPSHOTS_TOTAL,
            Unit::Count,
            "Current number of snapshots in the DuckLake catalog."
        );
        describe_gauge!(
            ETL_DUCKLAKE_OLDEST_SNAPSHOT_AGE_SECONDS,
            Unit::Seconds,
            "Age in seconds of the oldest snapshot in the DuckLake catalog."
        );
        describe_gauge!(
            ETL_DUCKLAKE_FILES_SCHEDULED_FOR_DELETION_TOTAL,
            Unit::Count,
            "Current number of files scheduled for deletion."
        );
        describe_gauge!(
            ETL_DUCKLAKE_FILES_SCHEDULED_FOR_DELETION_BYTES,
            Unit::Bytes,
            "Current total bytes of files scheduled for deletion."
        );
        describe_gauge!(
            ETL_DUCKLAKE_OLDEST_SCHEDULED_DELETION_AGE_SECONDS,
            Unit::Seconds,
            "Age in seconds of the oldest file waiting in files_scheduled_for_deletion."
        );
    });
}
