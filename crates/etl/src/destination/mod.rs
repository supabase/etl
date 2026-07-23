//! Data destination abstractions and implementations.
//!
//! Provides destination traits for sending replicated data to target systems.
//! Destinations handle both initial table synchronization data and streaming
//! replication events. Most destination authors implement [`Destination`]
//! directly. [`PipelineDestination`] is the blanket-implemented facade used by
//! the pipeline runtime when it needs cloneable, worker-safe destinations.
//! [`TaskSet`] is available for destinations that complete accepted writes in
//! background tasks while keeping shutdown and panic handling explicit.

mod async_result;
mod base;
mod capabilities;
mod metadata;

pub(crate) use async_result::{
    ApplyLoopAsyncResultMetadata, CompletedWriteEventsResult, EventsBatchMetrics,
    PendingWriteEventsResult,
};
pub use async_result::{
    DestinationWriteStatus, DropTableForCopyResult, WriteEventsDurability, WriteEventsResult,
    WriteTableRowsResult,
};
pub use base::Destination;
pub use capabilities::PipelineDestination;
pub use metadata::{
    AppliedDestinationTableMetadata, DestinationTableMetadata, DestinationTableSchemaStatus,
};

pub use crate::runtime::concurrency::{TaskSet, TaskSetDrainGuard};
