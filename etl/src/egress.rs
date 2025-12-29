//! Egress logging for billing and usage tracking.
//!
//! Provides structured logging for egress metrics with consistent field naming.
//! All egress logs include `egress_metric = true` for easy filtering in log aggregators.

/// Logs an egress metric at info level with `egress_metric = true` automatically included.
///
/// Accepts a message constant as the first argument followed by any key=value pairs.
/// The `egress_metric = true` field is always added to enable filtering egress logs.
#[macro_export]
macro_rules! egress_info {
    ($message:expr $(, $($fields:tt)*)?) => {
        tracing::info!(
            message = $message,
            egress_metric = true,
            $($($fields)*)?
        )
    };
}
