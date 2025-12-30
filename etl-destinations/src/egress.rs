//! Egress metric logging for destination implementations.
//!
//! Provides functions for logging billing-related egress metrics with a consistent schema.
//! All egress logs include `egress_metric = true` for filtering in log aggregators.

use etl::egress_info;

/// Log message for bytes processed and sent to a destination.
const ETL_PROCESSED_BYTES: &str = "etl_processed_bytes";

/// Processing type for initial table copy operations.
pub const PROCESSING_TYPE_TABLE_COPY: &str = "table_copy";

/// Processing type for ongoing CDC streaming operations.
pub const PROCESSING_TYPE_STREAMING: &str = "streaming";

/// Logs bytes processed metrics for billing purposes.
///
/// Records the number of bytes sent to and received from the destination.
/// For destinations that don't track bytes received, pass 0.
///
/// # Fields logged
/// - `message`: `"etl_processed_bytes"`
/// - `egress_metric`: `true`
/// - `destination_type`: the destination name (e.g., `"bigquery"`, `"iceberg"`)
/// - `processing_type`: `"table_copy"` or `"streaming"`
/// - `bytes_sent`: number of bytes sent to the destination
/// - `bytes_received`: number of bytes received from the destination (0 if not applicable)
///
/// Additionally, `project` and `pipeline_id` are auto-injected by the tracing writer.
pub fn log_processed_bytes(
    destination_type: &'static str,
    processing_type: &'static str,
    bytes_sent: u64,
    bytes_received: u64,
) {
    egress_info!(
        ETL_PROCESSED_BYTES,
        destination_type,
        processing_type,
        bytes_sent,
        bytes_received
    );
}
