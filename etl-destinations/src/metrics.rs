use std::sync::Once;

use metrics::{Unit, describe_counter, describe_gauge};

static REGISTER_METRICS: Once = Once::new();

pub const BQ_BATCH_SIZE: &str = "bq_batch_size";
pub const BQ_BATCH_SEND_MILLISECONDS_TOTAL: &str = "bq_batch_send_milliseconds_total";
pub const BQ_EGRESS_BYTES_TOTAL: &str = "bq_egress_bytes_total";

pub const SEND_PHASE: &str = "send_phase";
pub const TABLE_COPY: &str = "table_copy";
pub const APPLY: &str = "apply";

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

        describe_counter!(
            BQ_EGRESS_BYTES_TOTAL,
            Unit::Milliseconds,
            "Total bytes sent to BigQuery"
        );
    });
}
