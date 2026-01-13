//! Metrics definitions for ETL pipeline monitoring.

/// Label for pipeline ID in metrics.
pub const PIPELINE_ID_LABEL: &str = "pipeline_id";

/// Label for table name in metrics.
pub const TABLE_NAME_LABEL: &str = "table_name";

/// Label for error kind in metrics.
pub const ERROR_KIND_LABEL: &str = "error_kind";

// Heartbeat metrics

/// Counter for total heartbeat emissions.
pub const ETL_HEARTBEAT_EMISSIONS_TOTAL: &str = "etl_heartbeat_emissions_total";

/// Counter for total heartbeat failures.
pub const ETL_HEARTBEAT_FAILURES_TOTAL: &str = "etl_heartbeat_failures_total";

/// Gauge for consecutive heartbeat failures.
pub const ETL_HEARTBEAT_CONSECUTIVE_FAILURES: &str = "etl_heartbeat_consecutive_failures";

/// Counter for heartbeat connection attempts.
pub const ETL_HEARTBEAT_CONNECTION_ATTEMPTS_TOTAL: &str = "etl_heartbeat_connection_attempts_total";

/// Gauge for last heartbeat emission timestamp.
pub const ETL_HEARTBEAT_LAST_EMISSION_TIMESTAMP: &str = "etl_heartbeat_last_emission_timestamp";
