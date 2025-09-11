use metrics::{Unit, describe_histogram};
use std::sync::Once;

static REGISTER: Once = Once::new();

/// BigQuery-specific: duration to append batches via Storage Write API.
pub const BQ_APPEND_DURATION_SECONDS: &str = "bq_append_duration_seconds";

/// Register BigQuery-specific metrics. Safe to call multiple times.
pub fn register_metrics() {
    REGISTER.call_once(|| {
        describe_histogram!(
            BQ_APPEND_DURATION_SECONDS,
            Unit::Seconds,
            "Time taken in seconds by BigQuery Storage Write API append"
        );
    });
}
