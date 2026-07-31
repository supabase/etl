//! Billing usage logging.

/// Logs acknowledged source payload bytes for billing.
///
/// `bytes_sent` is the source payload that ETL treats as sent for billing once
/// the destination acknowledges the batch. It is not the destination-encoded
/// request size or the number of bytes written to the network.
pub(crate) fn log_processed_bytes(
    destination_type: &'static str,
    processing_type: &'static str,
    bytes_sent: u64,
) {
    tracing::info!(
        message = "etl_processed_bytes",
        egress_metric = true,
        destination_type,
        processing_type,
        bytes_sent
    );
}
