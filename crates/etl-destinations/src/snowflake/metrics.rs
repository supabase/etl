use std::sync::Once;

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

static REGISTER_METRICS: Once = Once::new();

pub(super) const ETL_SNOWFLAKE_BATCH_SIZE: &str = "etl_snowflake_batch_size";
pub(super) const ETL_SNOWFLAKE_BATCH_BYTES: &str = "etl_snowflake_batch_bytes";
pub(super) const ETL_SNOWFLAKE_INSERT_ERRORS_TOTAL: &str = "etl_snowflake_insert_errors_total";
pub(super) const ETL_SNOWFLAKE_APPEND_DURATION_SECONDS: &str =
    "etl_snowflake_append_duration_seconds";
pub(super) const ETL_SNOWFLAKE_ACCEPTED_BATCHES_TOTAL: &str =
    "etl_snowflake_accepted_batches_total";
pub(super) const ETL_SNOWFLAKE_ACCEPTED_ROWS_TOTAL: &str = "etl_snowflake_accepted_rows_total";
pub(super) const ETL_SNOWFLAKE_REJECTED_ROWS_TOTAL: &str = "etl_snowflake_rejected_rows_total";
pub(super) const FAILURE_TYPE_LABEL: &str = "failure_type";
pub(super) const ETL_SNOWFLAKE_CHANNEL_RECOVERIES_TOTAL: &str =
    "etl_snowflake_channel_recoveries_total";
pub(super) const ETL_SNOWFLAKE_STREAMING_PENDING_BYTES: &str =
    "etl_snowflake_streaming_pending_bytes";
pub(super) const ETL_SNOWFLAKE_STREAMING_PENDING_ROW_BATCHES: &str =
    "etl_snowflake_streaming_pending_row_batches";
pub(super) const ETL_SNOWFLAKE_STREAMING_PENDING_CHANNELS: &str =
    "etl_snowflake_streaming_pending_channels";
pub(super) const ETL_SNOWFLAKE_STREAMING_DURABILITY_WAIT_SECONDS: &str =
    "etl_snowflake_streaming_durability_wait_seconds";
pub(super) const ETL_SNOWFLAKE_STREAMING_DURABILITY_WAIT_FAILURES_TOTAL: &str =
    "etl_snowflake_streaming_durability_wait_failures_total";

pub(super) fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        describe_histogram!(
            ETL_SNOWFLAKE_BATCH_SIZE,
            Unit::Count,
            "Rows per Snowpipe Streaming append request"
        );

        describe_histogram!(
            ETL_SNOWFLAKE_BATCH_BYTES,
            Unit::Bytes,
            "Batch size in bytes (compressed)"
        );

        describe_counter!(
            ETL_SNOWFLAKE_INSERT_ERRORS_TOTAL,
            Unit::Count,
            "Total failed Snowpipe append operations, labeled by failure_type."
        );

        describe_histogram!(
            ETL_SNOWFLAKE_APPEND_DURATION_SECONDS,
            Unit::Seconds,
            "Duration of one Snowpipe append operation, including authentication refresh, \
             retries, and stale-channel recovery."
        );

        describe_counter!(
            ETL_SNOWFLAKE_ACCEPTED_BATCHES_TOTAL,
            Unit::Count,
            "Total row batches newly accepted by Snowflake; acceptance does not prove durability."
        );

        describe_counter!(
            ETL_SNOWFLAKE_ACCEPTED_ROWS_TOTAL,
            Unit::Count,
            "Total rows in batches newly accepted by Snowflake; acceptance does not prove \
             durability."
        );

        describe_counter!(
            ETL_SNOWFLAKE_REJECTED_ROWS_TOTAL,
            Unit::Count,
            "Total rows newly reported as rejected by Snowflake channel status."
        );

        describe_counter!(
            ETL_SNOWFLAKE_CHANNEL_RECOVERIES_TOTAL,
            Unit::Count,
            "Stale channel recovery count"
        );

        describe_gauge!(
            ETL_SNOWFLAKE_STREAMING_PENDING_BYTES,
            Unit::Bytes,
            "Accepted Snowflake streaming bytes awaiting durability proof"
        );

        describe_gauge!(
            ETL_SNOWFLAKE_STREAMING_PENDING_ROW_BATCHES,
            Unit::Count,
            "Accepted Snowflake streaming row batches awaiting durability proof"
        );

        describe_gauge!(
            ETL_SNOWFLAKE_STREAMING_PENDING_CHANNELS,
            Unit::Count,
            "Snowflake streaming channels with accepted work awaiting durability proof"
        );

        describe_histogram!(
            ETL_SNOWFLAKE_STREAMING_DURABILITY_WAIT_SECONDS,
            Unit::Seconds,
            "Snowflake streaming durability wait duration"
        );

        describe_counter!(
            ETL_SNOWFLAKE_STREAMING_DURABILITY_WAIT_FAILURES_TOTAL,
            Unit::Count,
            "Total Snowflake streaming durability wait failures"
        );
    });
}
