//! Task ownership, completion, and shutdown.
//!
//! [`TaskGroup`] owns the fallible children of one operation. [`TaskRegistry`]
//! is a shared registry for background tasks whose outputs travel through other
//! channels. Both join aborted siblings before returning a task failure.

mod group;
mod registry;

pub use group::TaskGroup;
pub use registry::{TaskRegistry, TaskRegistryDrainGuard};
use tokio_util::task::AbortOnDropHandle;

use crate::error::EtlResult;

/// Aborts an owned disposable task and waits for it to release its resources.
///
/// Deliberate cancellation returns `None`; a task that already completed
/// returns its output. Panics remain errors, and task-returned errors remain in
/// the output for the caller to propagate. The handle remains guarded while
/// joining; cancelling this future drops the guard without waiting for
/// completion.
///
/// Use this only when the owner intends to abort the task. Graceful joins must
/// still report unexpected cancellation as a task failure.
pub async fn abort_and_join<T>(task: AbortOnDropHandle<T>) -> EtlResult<Option<T>> {
    task.abort();

    match task.await {
        Ok(output) => Ok(Some(output)),
        Err(error) if error.is_cancelled() => Ok(None),
        Err(error) => Err(error.into()),
    }
}
