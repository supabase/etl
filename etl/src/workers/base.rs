use etl_postgres::schema::TableId;
use std::future::Future;

use crate::error::EtlResult;

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
    fn wait(self) -> impl Future<Output = EtlResult<()>> + Send;
}
