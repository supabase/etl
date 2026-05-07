//! Fault injection utilities for testing error scenarios.
//!
//! Provides configurable failpoints that can trigger specific error conditions
//! during testing. Failpoints support different retry policies to test error
//! handling and recovery behavior.

use fail::fail_point;

use crate::{
    bail,
    error::{ErrorKind, EtlResult},
};

pub const START_TABLE_SYNC_BEFORE_DATA_SYNC_SLOT_CREATION_FP: &str =
    "start_table_sync.before_data_sync_slot_creation_fp";
pub const START_TABLE_SYNC_DURING_DATA_SYNC_FP: &str = "start_table_sync.during_data_sync_fp";
pub const START_TABLE_SYNC_AFTER_FINISHED_COPY_FP: &str = "start_table_sync.after_finished_copy_fp";
pub const APPLY_LOOP_HANDLE_RELATION_MESSAGE_AFTER_SKIPPED_RELATION_FP: &str =
    "apply_loop.handle_relation_message.after_skipped_relation_fp";
pub const APPLY_WORKER_PROCESS_SINGLE_SYNCING_TABLE_AFTER_COMMIT_AFTER_CATCHUP_STARTED_FP: &str =
    "apply_worker.process_single_syncing_table_after_commit.after_catchup_started_fp";
pub const APPLY_WORKER_PROCESS_SINGLE_SYNCING_TABLE_WHEN_IDLE_AFTER_CATCHUP_STARTED_FP: &str =
    "apply_worker.process_single_syncing_table_when_idle.after_catchup_started_fp";
pub const TABLE_SYNC_WORKER_TRY_COMPLETE_CATCHUP_BEFORE_SYNC_DONE_FP: &str =
    "table_sync_worker.try_complete_catchup.before_sync_done_fp";
pub const SEND_STATUS_UPDATE_FP: &str = "send_status_update_fp";
pub const FORCE_SCHEMA_CLEANUP_FP: &str = "force_schema_cleanup_fp";

/// Executes a configurable failpoint for testing error scenarios.
///
/// When the failpoint is active, and it's set to return an error, this function
/// generates an [`crate::error::EtlError`] with the specified retry policy. The
/// retry behavior can be controlled through the failpoint parameter:
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
                "manual_retry" => ErrorKind::WithManualRetry,
                "timed_retry" => ErrorKind::WithTimedRetry,
                _ => ErrorKind::WithNoRetry,
            }
        }

        bail!(
            error_kind,
            "Failpoint triggered an error",
            format!("Failpoint '{}' returned an error", name)
        );
    });

    Ok(())
}

/// Returns `true` if a specific failpoint is active, `false` otherwise.
///
/// A failpoint is considered active if it throws an error.
pub fn etl_fail_point_active(name: &str) -> bool {
    etl_fail_point(name).is_err()
}
