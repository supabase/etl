//! Data destination abstractions and implementations.
//!
//! Provides destination traits for sending replicated data to target systems.
//! Destinations handle both initial table synchronization data and streaming
//! replication events. The [`capabilities`] module provides facade traits for
//! common runtime destination roles.

pub mod async_result;
mod base;
pub mod capabilities;

pub use async_result::{DropTableForCopyResult, WriteEventsResult, WriteTableRowsResult};
pub use base::Destination;

pub use crate::materialization::{DestinationMaterializationPolicy, TypeStrategy, ValueStrategy};
pub use capabilities::PipelineDestination;
