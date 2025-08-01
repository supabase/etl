//! Failpoint integration for testing error scenarios in the ETL pipeline.
//!
//! Provides utilities for injecting controlled failures during testing to verify
//! error handling and retry behavior. Uses the `fail` crate for configurable
//! failpoints that can be activated from tests.

use fail::fail_point;

use crate::bail;
use crate::error::{ErrorKind, EtlError, EtlResult};

pub const START_TABLE_SYNC__AFTER_DATA_SYNC: &str = "start_table_sync.after_data_sync";

/// Executes a configurable failpoint for testing error scenarios.
///
/// When the failpoint is active, and it set to return an error, this function generates an [`EtlError`] with
/// the specified retry policy. The retry behavior can be controlled through
/// the failpoint parameter:
///
/// - `"no_retry"` - Creates an error that should not be retried
/// - `"manual_retry"` - Creates an error requiring manual intervention
/// - `"timed_retry"` - Creates an error that can be automatically retried
/// - Any other value defaults to `"no_retry"`
///
/// Returns `Ok(())` when the failpoint is inactive, allowing normal execution.
pub fn etl_fail_point(name: &str) -> EtlResult<()> {
    fail_point!(name, |parameter| {
        let mut error_kind = ErrorKind::WithNoRetry;
        if let Some(parameter) = parameter {
            error_kind = match parameter.as_str() {
                "no_retry" => ErrorKind::WithNoRetry,
                "manual_retry" => ErrorKind::WithManualRetry,
                "timed_retry" => ErrorKind::WithTimedRetry,
                _ => ErrorKind::WithNoRetry,
            }
        }

        bail!(
            error_kind,
            "An error occurred in a fail point",
            format!("The failpoint '{name}' returned an error")
        );
    });

    Ok(())
}
