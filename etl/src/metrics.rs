use metrics::{Unit, describe_counter, describe_gauge};

pub const ETL_TABLES_TOTAL: &str = "etl_tables_total";
pub const ETL_TABLE_SYNC_ROWS_COPIED_TOTAL: &str = "etl_table_sync_rows_copied_total";
pub const ETL_APPLY_EVENTS_COPIED_TOTAL: &str = "etl_apply_events_copied_total";

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
}
