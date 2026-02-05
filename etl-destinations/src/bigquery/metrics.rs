use std::sync::Once;

use metrics::{Unit, describe_counter, describe_histogram};

static REGISTER_METRICS: Once = Once::new();

// Core metrics - only what's essential for debugging
pub const ETL_BQ_APPEND_BATCHES_BATCH_SIZE: &str = "etl_bq_append_batches_batch_size";
pub const ETL_BQ_APPEND_BATCHES_BATCH_RETRIES_TOTAL: &str =
    "etl_bq_append_batches_batch_retries_total";
pub const ETL_BQ_APPEND_BATCHES_BATCH_ERRORS_TOTAL: &str =
    "etl_bq_append_batches_batch_errors_total";
pub const ETL_BQ_APPEND_BATCHES_BATCH_ROW_ERRORS_TOTAL: &str =
    "etl_bq_append_batches_batch_row_errors_total";

/// Register BigQuery-specific metrics.
///
/// This should be called before starting BigQuery operations.
/// It is safe to call this method multiple times - metrics are registered only once.
pub fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        describe_histogram!(
            ETL_BQ_APPEND_BATCHES_BATCH_SIZE,
            Unit::Count,
            "Distribution of batch sizes (number of rows per batch)"
        );

        describe_counter!(
            ETL_BQ_APPEND_BATCHES_BATCH_RETRIES_TOTAL,
            Unit::Count,
            "Total batch retries, labeled by error_code and attempt"
        );

        describe_counter!(
            ETL_BQ_APPEND_BATCHES_BATCH_ERRORS_TOTAL,
            Unit::Count,
            "Total append_batches errors from BigQuery, labeled by error_code and retryable"
        );

        describe_counter!(
            ETL_BQ_APPEND_BATCHES_BATCH_ROW_ERRORS_TOTAL,
            Unit::Count,
            "Total append_batches row-level errors"
        );
    });
}
