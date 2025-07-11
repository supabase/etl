use postgres::schema::TableId;
use std::fmt;
use std::future::Future;
use thiserror::Error;
use tokio::task;

use crate::workers::apply::ApplyWorkerError;
use crate::workers::table_sync::TableSyncWorkerError;

/// Represents all possible errors that can occur while waiting for a worker to complete.
///
/// This enum is used to distinguish between different failure modes that may arise during
/// the execution of asynchronous worker tasks, including panics, explicit errors, and
/// silent failures that are internally handled but still indicate abnormal termination.
#[derive(Debug, Error)]
pub enum WorkerWaitError {
    /// The worker's task panicked, causing its join handle to return an error.
    ///
    /// This typically indicates a bug or unrecoverable condition in the worker's logic.
    #[error("Worker task panicked or was forcefully aborted: {0}")]
    WorkerPanicked(#[from] task::JoinError),

    /// The worker's task failed internally, but the error was caught and not propagated.
    ///
    /// This can occur when using abstractions like `ReactiveFuture`, which catch both panics and
    /// `Result::Err` values, and then notify the pool about the failure without letting it bubble up.
    #[error("Worker task failed internally and the error was silently handled: {0}")]
    WorkerSilentlyFailed(String),

    /// The apply worker encountered an error that was propagated via the handle's return value.
    ///
    /// This variant wraps the specific error returned by the apply worker.
    #[error("Apply worker terminated with an error: {0}")]
    ApplyWorkerFailed(#[from] ApplyWorkerError),

    /// The table sync worker encountered an error that was propagated via the handle's return value.
    ///
    /// This variant wraps the specific error returned by the table sync worker.
    #[error("Table sync worker terminated with an error: {0}")]
    TableSyncWorkerFailed(#[from] TableSyncWorkerError),
}

#[derive(Debug)]
pub struct WorkerWaitErrors(pub Vec<WorkerWaitError>);

impl fmt::Display for WorkerWaitErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            write!(f, "no worker failed.")
        } else {
            writeln!(f, "the workers failed with the following errors:")?;
            for (i, err) in self.0.iter().enumerate() {
                writeln!(f, "  {}: {}", i + 1, err)?;
            }
            Ok(())
        }
    }
}

/// The type of worker that is currently running.
///
/// A worker type can also have properties that uniquely identify it.
#[derive(Debug, Copy, Clone)]
pub enum WorkerType {
    Apply,
    TableSync { table_id: TableId },
}

/// A trait for types that can be started as workers.
///
/// The generic parameter `H` represents the handle type that will be returned when the worker starts,
/// and `S` represents the state type that can be accessed through the handle.
pub trait Worker<H, S>
where
    H: WorkerHandle<S>,
{
    /// Error type.
    type Error;

    /// Starts the worker and returns a future that resolves to an optional handle.
    ///
    /// The handle can be used to monitor and control the worker's execution.
    fn start(self) -> impl Future<Output = Result<H, Self::Error>> + Send;
}

/// A handle to a running worker that provides access to its state and completion status.
///
/// The generic parameter `S` represents the type of state that can be accessed through this handle.
pub trait WorkerHandle<S> {
    /// Returns the current state of the worker.
    ///
    /// Note that the state of the worker is expected to NOT be tied with its lifetime, so if you
    /// hold a reference to the state, it won't say anything about the worker's status, however it
    /// could be used to encode it's state but this is based on the semantics of the concrete type
    /// and not this abstraction.
    fn state(&self) -> S;

    /// Returns a future that resolves when the worker completes.
    ///
    /// The future resolves to a [`Result`] indicating whether the worker completed successfully
    /// or encountered an error.
    fn wait(self) -> impl Future<Output = Result<(), WorkerWaitError>> + Send;
}
