//! Data destination abstractions and implementations.
//!
//! Provides the [`Destination`] trait and implementations for sending replicated data to target systems.
//! Destinations handle both initial table synchronization data and streaming replication events.

mod async_result;
mod base;

pub use async_result::{ApplyAsyncResult, CompletedApplyAsyncResult, PendingApplyAsyncResult};
pub use base::Destination;
