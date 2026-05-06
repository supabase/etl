use std::sync::Once;

use metrics::{Unit, describe_counter, describe_histogram};

static REGISTER_METRICS: Once = Once::new();

pub(super) const ETL_SNOWFLAKE_BATCH_SIZE: &str = "etl_snowflake_batch_size";
pub(super) const ETL_SNOWFLAKE_BATCH_BYTES: &str = "etl_snowflake_batch_bytes";
pub(super) const ETL_SNOWFLAKE_INSERT_ERRORS_TOTAL: &str = "etl_snowflake_insert_errors_total";
pub(super) const ETL_SNOWFLAKE_CHANNEL_RECOVERIES_TOTAL: &str =
    "etl_snowflake_channel_recoveries_total";

pub(super) fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        describe_histogram!(ETL_SNOWFLAKE_BATCH_SIZE, Unit::Count, "Rows per insert_rows request");

        describe_histogram!(
            ETL_SNOWFLAKE_BATCH_BYTES,
            Unit::Bytes,
            "Batch size in bytes (pre-compression)"
        );

        describe_counter!(
            ETL_SNOWFLAKE_INSERT_ERRORS_TOTAL,
            Unit::Count,
            "Total insert_rows errors from Snowpipe Streaming"
        );

        describe_counter!(
            ETL_SNOWFLAKE_CHANNEL_RECOVERIES_TOTAL,
            Unit::Count,
            "Channel recovery count (GC'd or errored channels)"
        );
    });
}
