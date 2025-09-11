use metrics::{Unit, describe_histogram};
use std::sync::Once;

static REGISTER: Once = Once::new();

/// Duration to append batches via Storage Write API.
pub const ETL_BIGQUERY_APPEND_DURATION_MILLISECONDS: &str =
    "etl_bigquery_append_duration_milliseconds";

/// Register BigQuery-specific metrics. Safe to call multiple times.
pub fn register_metrics() {
    REGISTER.call_once(|| {
        describe_histogram!(
            ETL_BIGQUERY_APPEND_DURATION_MILLISECONDS,
            Unit::Milliseconds,
            "Time taken in milliseconds by BigQuery Storage Write API append"
        );
    });
}
