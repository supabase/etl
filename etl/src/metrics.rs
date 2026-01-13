use metrics::{Unit, describe_counter, describe_gauge};
use std::sync::Once;

// Pipeline-level metrics.
pub const ETL_TABLES_COUNT: &str = "etl_tables_count";
pub const ETL_TABLE_COPY_ROWS_TOTAL: &str = "etl_table_copy_rows_total";
pub const ETL_TABLE_STATE: &str = "etl_table_state";
pub const ETL_REPLICATION_LAG: &str = "etl_replication_lag";

// Event processing metrics.
pub const ETL_EVENTS_APPLIED_TOTAL: &str = "etl_events_applied_total";
pub const ETL_EVENTS_PROCESSED_TOTAL: &str = "etl_events_processed_total";
pub const ETL_STATUS_UPDATES_TOTAL: &str = "etl_status_updates_total";
pub const ETL_STATUS_UPDATES_SKIPPED_TOTAL: &str = "etl_status_updates_skipped_total";

// Heartbeat metrics for read replica support.
pub const ETL_HEARTBEAT_EMISSIONS_TOTAL: &str = "etl_heartbeat_emissions_total";
pub const ETL_HEARTBEAT_FAILURES_TOTAL: &str = "etl_heartbeat_failures_total";
pub const ETL_HEARTBEAT_CONNECTION_ATTEMPTS_TOTAL: &str = "etl_heartbeat_connection_attempts_total";
pub const ETL_HEARTBEAT_LAST_EMISSION_TIMESTAMP: &str = "etl_heartbeat_last_emission_timestamp";
pub const ETL_HEARTBEAT_CONSECUTIVE_FAILURES: &str = "etl_heartbeat_consecutive_failures";

/// Label key for replication phase (used by table state metrics).
pub const PHASE_LABEL: &str = "phase";
/// Label key for the ETL worker type ("table_sync" or "apply").
pub const WORKER_TYPE_LABEL: &str = "worker_type";
/// Label key for the pipeline identifier.
pub const PIPELINE_ID_LABEL: &str = "pipeline_id";
/// Label key for the table identifier.
pub const TABLE_ID_LABEL: &str = "table_id";

static INIT: Once = Once::new();

/// Registers metric definitions with the metrics registry.
///
/// This function is idempotent and will only register metrics once.
/// It should be called during application initialization before any metrics are recorded.
pub(crate) fn register_metrics() {
    INIT.call_once(|| {
        describe_gauge!(
            ETL_TABLES_COUNT,
            Unit::Count,
            "Number of tables being replicated in the pipeline, labeled by pipeline_id"
        );

        describe_counter!(
            ETL_TABLE_COPY_ROWS_TOTAL,
            Unit::Count,
            "Total number of rows copied during table sync, labeled by pipeline_id and table_id"
        );

        describe_gauge!(
            ETL_TABLE_STATE,
            Unit::Count,
            "Current replication phase of each table (0=not started, 1=copying, 2=streaming), labeled by pipeline_id, table_id, and phase"
        );

        describe_gauge!(
            ETL_REPLICATION_LAG,
            Unit::Seconds,
            "Current replication lag in seconds, labeled by pipeline_id"
        );

        describe_counter!(
            ETL_EVENTS_APPLIED_TOTAL,
            Unit::Count,
            "Total number of events applied to destination, labeled by pipeline_id and worker_type"
        );

        describe_counter!(
            ETL_EVENTS_PROCESSED_TOTAL,
            Unit::Count,
            "Total number of events processed from the replication stream, labeled by pipeline_id"
        );

        describe_counter!(
            ETL_STATUS_UPDATES_TOTAL,
            Unit::Count,
            "Total number of LSN status updates sent to Postgres, labeled by pipeline_id"
        );

        describe_counter!(
            ETL_STATUS_UPDATES_SKIPPED_TOTAL,
            Unit::Count,
            "Total number of status updates skipped due to throttling, labeled by pipeline_id"
        );

        // Heartbeat metrics for read replica support.
        describe_counter!(
            ETL_HEARTBEAT_EMISSIONS_TOTAL,
            Unit::Count,
            "Total number of heartbeat messages emitted to the primary, labeled by pipeline_id"
        );

        describe_counter!(
            ETL_HEARTBEAT_FAILURES_TOTAL,
            Unit::Count,
            "Total number of heartbeat failures, labeled by pipeline_id"
        );

        describe_counter!(
            ETL_HEARTBEAT_CONNECTION_ATTEMPTS_TOTAL,
            Unit::Count,
            "Total number of heartbeat connection attempts to the primary, labeled by pipeline_id"
        );

        describe_gauge!(
            ETL_HEARTBEAT_LAST_EMISSION_TIMESTAMP,
            Unit::Seconds,
            "Unix timestamp of the last successful heartbeat emission, labeled by pipeline_id"
        );

        describe_gauge!(
            ETL_HEARTBEAT_CONSECUTIVE_FAILURES,
            Unit::Count,
            "Number of consecutive heartbeat failures, labeled by pipeline_id"
        );
    });
}
