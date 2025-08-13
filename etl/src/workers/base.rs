use etl_postgres::schema::TableId;
use std::future::Future;

use crate::error::EtlResult;

/// Classification of ETL worker types with identifying properties.
///
/// [`WorkerType`] enables distinguishing between different categories of workers
/// in the ETL system, each handling specific aspects of the replication process.
/// This is useful for logging, monitoring, and worker management.
#[derive(Debug, Copy, Clone)]
pub enum WorkerType {
    /// Worker that applies replication stream events to destinations
    Apply,
    /// Worker that synchronizes data for a specific table during initial load
    TableSync {
        /// Unique identifier of the table being synchronized
        table_id: TableId,
    },
}

/// Trait for background workers in the ETL system.
///
/// [`Worker`] defines the interface for starting and managing background workers
/// that handle different aspects of ETL processing. Workers return handles that
/// can be used to monitor their progress and wait for completion.
///
/// The generic parameter `H` represents the handle type returned when the worker starts,
/// and `S` represents the state type accessible through the handle.
pub trait Worker<H, S>
where
    H: WorkerHandle<S>,
{
    /// Error type returned when worker startup fails.
    type Error;

    /// Starts the worker and returns a handle for monitoring its execution.
    ///
    /// This method begins background processing and returns immediately with a handle
    /// that can be used to monitor progress and wait for completion.
    fn start(self) -> impl Future<Output = Result<H, Self::Error>> + Send;
}

/// Handle for monitoring and controlling a running worker.
///
/// [`WorkerHandle`] provides access to worker state and enables waiting for worker
/// completion. The handle remains valid even after the worker completes, allowing
/// for state inspection and result retrieval.
///
/// The generic parameter `S` represents the type of state accessible through this handle.
pub trait WorkerHandle<S> {
    /// Returns the current state of the worker.
    ///
    /// The state represents a snapshot of the worker's current status and progress.
    /// Note that the state is independent of the worker's lifetime - holding a state
    /// reference doesn't prevent the worker from completing or provide guarantees
    /// about the worker's current status.
    fn state(&self) -> S;

    /// Waits for the worker to complete and returns the final result.
    ///
    /// This method blocks until the worker finishes processing and returns a result
    /// indicating whether the worker completed successfully or encountered an error.
    /// The handle is consumed by this operation.
    fn wait(self) -> impl Future<Output = EtlResult<()>> + Send;
}
