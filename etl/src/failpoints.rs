use fail::fail_point;

use crate::bail;
use crate::error::{ErrorKind, EtlError, EtlResult};

pub const START_TABLE_SYNC__AFTER_DATA_SYNC: &str = "start_table_sync.after_data_sync";

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
