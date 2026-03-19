//! Data destination abstractions and implementations.
//!
//! Provides the [`Destination`] trait and implementations for sending replicated data to target systems.
//! Destinations handle both initial table synchronization data and streaming replication events.

mod base;
mod flush_result;
mod task_set;

pub use base::Destination;
pub use flush_result::{
    BatchFlushMetrics, BatchFlushResult, CompletedBatchFlushResult, PendingBatchFlushResult,
};
pub use task_set::DestinationTaskSet;
