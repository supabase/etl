use metrics::{Unit, describe_gauge};

pub const ETL_TABLES_TOTAL: &str = "etl_tables_total";

/// Register metrics emitted by etl. This method should be called
/// early during initialization by the binary using the etl crate.
/// If this method is not called during initialization, the metrics
/// will still be emitted, but they will not have their descriptions
/// returned with the metrics.
pub fn register_metrics() {
    describe_gauge!(
        ETL_TABLES_TOTAL,
        Unit::Count,
        "Total number of tables being copied by etl"
    );
}
