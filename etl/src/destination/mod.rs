//! Data destination abstractions and implementations.
//!
//! Provides the [`Destination`] trait and implementations for sending replicated data to target systems.
//! Destinations handle both initial table synchronization data and streaming replication events.

pub mod async_result;
mod base;
pub mod task_set;

pub use base::Destination;
