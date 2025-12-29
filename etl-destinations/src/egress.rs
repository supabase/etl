//! Egress metric constants for destination implementations.
//!
//! Defines log message constants for tracking data egress across all destinations.
//! Use these with the [`etl::egress_info!`] macro for consistent billing logs.

/// Log message for bytes processed and sent to a destination.
pub const ETL_PROCESSED_BYTES: &str = "etl_processed_bytes";
