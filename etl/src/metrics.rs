use std::sync::Once;

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

static REGISTER_METRICS: Once = Once::new();

pub const ETL_TABLES_TOTAL: &str = "etl_tables_total";
pub const ETL_ITEMS_COPIED_TOTAL: &str = "etl_items_copied_total";
pub const ETL_BATCH_SIZE: &str = "etl_batch_size";
pub const ETL_BATCH_SEND_DURATION_SECONDS: &str = "etl_batch_send_duration_seconds";
pub const MILLIS_PER_SEC: f64 = 1_000.0;
pub const PHASE: &str = "phase";
pub const TABLE_SYNC: &str = "table_sync";
pub const APPLY: &str = "apply";
pub const DESTINATION: &str = "destination";

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
            ETL_ITEMS_COPIED_TOTAL,
            Unit::Count,
            "Total number of rows or events copied to destination in table sync or apply phase"
        );

        describe_gauge!(
            ETL_BATCH_SIZE,
            Unit::Count,
            "Batch size of events sent to the destination"
        );

        describe_histogram!(
            ETL_BATCH_SEND_DURATION_SECONDS,
            Unit::Seconds,
            "Time taken in seconds to send a batch of events to the destination"
        );
    });
}
