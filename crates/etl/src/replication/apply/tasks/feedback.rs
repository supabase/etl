//! Background feedback task startup and teardown.

use std::future::Future;

use tokio::task::JoinHandle;
use tracing::warn;

use crate::error::EtlResult;

/// Spawns the feedback sender, logging its failure before the task exits.
///
/// The stream supplies a fallible future, but its task has a unit result like
/// the other apply-loop tasks. The next feedback enqueue detects the closed
/// channel and fails the apply loop through its normal retry policy.
pub(super) fn spawn_feedback_task<F>(feedback_sender_future: F) -> JoinHandle<()>
where
    F: Future<Output = EtlResult<()>> + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(error) = feedback_sender_future.await {
            warn!(error = %error, "replication feedback sender failed");
        }
    })
}

/// Joins the stopped feedback sender and warns on unexpected task failures.
pub(super) async fn join(task: &mut JoinHandle<()>) {
    if let Err(error) = task.await
        && !error.is_cancelled()
    {
        warn!(error = %error, "replication feedback sender task failed before completing");
    }
}
