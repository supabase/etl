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
pub(crate) mod durability;
mod metadata;

pub(crate) use async_result::{
    ApplyLoopAsyncResultMetadata, CompletedWriteEventsResult, DispatchMetrics,
    PendingWriteEventsResult, PendingWriteTableRowsResult,
};
pub use async_result::{
    DropTableForCopyResult, FinishTableCopyResult, WriteEventsResult, WriteTableRowsResult,
};
pub use base::Destination;
pub use capabilities::PipelineDestination;
pub(crate) use durability::DestinationDurabilityEvents;
pub use durability::{
    DeferredStreamingConfig, DeferredTableCopyConfig, DestinationBatchId,
    DestinationDurabilityEvent, DestinationDurabilityReporter, StreamingDurabilityMode,
    TableCopyBatchId, TableCopyDurabilityMode, TrackedEventsBatch, TrackedTableRowsBatch,
};
pub use metadata::{
    AppliedDestinationTableMetadata, DestinationTableMetadata, DestinationTableSchemaStatus,
};

pub use crate::runtime::concurrency::TaskSet;
