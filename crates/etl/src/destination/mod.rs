//! Data destination abstractions and implementations.
//!
//! Provides destination traits for sending replicated data to target systems.
//! Destinations handle both initial table synchronization data and streaming
//! replication events. The [`capabilities`] module provides facade traits for
//! common runtime destination roles.

pub mod async_result;
mod base;
pub mod capabilities;
pub mod task_set;

pub use async_result::{
    TruncateTableResult, WriteEventsResult, WriteSnapshotBatchResult, WriteStreamBatchesResult,
    WriteTableRowsResult,
};
pub use base::Destination;
pub use capabilities::PipelineDestination;
