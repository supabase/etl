use std::sync::Once;

use metrics::{Unit, describe_gauge};

static REGISTER_METRICS: Once = Once::new();

pub const BQ_BATCH_SIZE: &str = "bq_batch_size";
pub const BQ_BATCH_SEND_MILLISECONDS_TOTAL: &str = "bq_batch_send_milliseconds_total";

/// Register metrics emitted by the destinations. It is safe to call
/// this method multiple times. It is guaraneed to register the
/// metrics only once.
pub(crate) fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        describe_gauge!(
            BQ_BATCH_SIZE,
            Unit::Count,
            "Batch size of events sent to BigQuery"
        );

        describe_gauge!(
            BQ_BATCH_SEND_MILLISECONDS_TOTAL,
            Unit::Milliseconds,
            "Time taken in milliseconds to send a batch of events to BigQuery"
        );
    });
}
