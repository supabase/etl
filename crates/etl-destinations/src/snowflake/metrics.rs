use std::sync::Once;

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

static REGISTER_METRICS: Once = Once::new();

pub(super) const ETL_SNOWFLAKE_BATCH_SIZE: &str = "etl_snowflake_batch_size";
pub(super) const ETL_SNOWFLAKE_BATCH_BYTES: &str = "etl_snowflake_batch_bytes";
pub(super) const ETL_SNOWFLAKE_INSERT_ERRORS_TOTAL: &str = "etl_snowflake_insert_errors_total";
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
        describe_histogram!(ETL_SNOWFLAKE_BATCH_SIZE, Unit::Count, "Rows per insert_rows request");

        describe_histogram!(
            ETL_SNOWFLAKE_BATCH_BYTES,
            Unit::Bytes,
            "Batch size in bytes (compressed)"
        );

        describe_counter!(
            ETL_SNOWFLAKE_INSERT_ERRORS_TOTAL,
            Unit::Count,
            "Total insert_rows errors from Snowpipe Streaming"
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
