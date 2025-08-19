use std::sync::Once;

use metrics::{Unit, describe_counter, describe_gauge};

static REGISTER_METRICS: Once = Once::new();

pub const ETL_TABLES_TOTAL: &str = "etl_tables_total";
pub const ETL_TABLE_SYNC_ROWS_COPIED_TOTAL: &str = "etl_table_sync_rows_copied_total";
pub const ETL_APPLY_EVENTS_COPIED_TOTAL: &str = "etl_apply_events_copied_total";
pub const ETL_BATCH_SIZE: &str = "etl_batch_size";
pub const ETL_BATCH_SEND_MILLISECONDS_TOTAL: &str = "etl_batch_send_milliseconds_total";

/// Register metrics emitted by etl. This should be called before starting a pipeline.
/// It is safe to call this method multiple times. It is guaraneed to register the
/// metrics only once.
pub(crate) fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        describe_gauge!(
            ETL_TABLES_TOTAL,
            Unit::Count,
            "Total number of tables being copied"
        );

        describe_counter!(
            ETL_TABLE_SYNC_ROWS_COPIED_TOTAL,
            Unit::Count,
            "Total number of rows copied to destination during table sync"
        );

        describe_counter!(
            ETL_APPLY_EVENTS_COPIED_TOTAL,
            Unit::Count,
            "Total number of events copied to destination in apply loop"
        );

        describe_gauge!(
            ETL_BATCH_SIZE,
            Unit::Count,
            "Batch size of events sent to the destination"
        );

        describe_gauge!(
            ETL_BATCH_SEND_MILLISECONDS_TOTAL,
            Unit::Milliseconds,
            "Time taken in milliseconds to send a batch of events to the destination"
        );
    });
}
