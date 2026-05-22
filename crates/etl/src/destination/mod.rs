//! Data destination abstractions and implementations.
//!
//! Provides destination traits for sending replicated data to target systems.
//! Destinations handle both initial table synchronization data and streaming
//! replication events.

pub mod async_result;
mod base;
pub mod pipeline;

pub use async_result::{DropTableForCopyResult, WriteEventsResult, WriteTableRowsResult};
pub use base::Destination;
pub use pipeline::PipelineDestination;
