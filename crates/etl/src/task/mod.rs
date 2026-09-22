//! Task ownership, completion, and shutdown.
//!
//! [`TaskGroup`] owns the fallible children of one operation. [`TaskRegistry`]
//! is a shared registry for background tasks whose outputs travel through other
//! channels. Both return the first failure and drop remaining tasks to request
//! cancellation without waiting. Controlled teardown joins owned tasks until
//! the first failure. Owner-requested cancellation is accepted silently;
//! panics and task-returned errors remain failures during shutdown.

mod group;
mod registry;

pub use group::TaskGroup;
pub use registry::{TaskRegistry, TaskRegistryDrainGuard};
use tokio_util::task::AbortOnDropHandle;

use crate::error::EtlResult;

/// Aborts an owned disposable task and waits for it to release its resources.
///
/// Requested cancellation silently returns `None`; completed tasks return their
/// output, including any task error. Panics propagate as errors. Cancelling
/// this future drops the guarded handle without waiting.
///
/// Use only for intentional aborts; graceful joins must report unexpected
/// cancellation as a failure.
pub async fn abort_and_join<T>(task: AbortOnDropHandle<T>) -> EtlResult<Option<T>> {
    task.abort();

    match task.await {
        Ok(output) => Ok(Some(output)),
        Err(error) if error.is_cancelled() => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Aborts and joins an owned disposable task returning [`EtlResult`].
///
/// Composes [`abort_and_join`], treating requested cancellation as success and
/// propagating both panics and task-returned errors.
pub async fn abort_and_join_result(task: AbortOnDropHandle<EtlResult<()>>) -> EtlResult<()> {
    abort_and_join(task).await?.unwrap_or(Ok(()))
}
