//! Common utilities and helpers for testing PostgreSQL replication functionality.
//!
//! This module provides shared testing infrastructure including database management,
//! pipeline testing utilities, destination testing helpers, and table manipulation utilities.
//! It also includes common testing patterns like waiting for conditions to be met.
pub mod database;
pub mod event;
pub mod pipeline;
pub mod table;
pub mod test_destination_wrapper;
pub mod test_schema;

// We export some types that are only useful when requiring the test utils since these types can be
// used in tests. They are not exported by default because they leak internals of the library.
pub use crate::state::store::notify::NotifyingStateStore;
pub use crate::state::table::{TableReplicationPhase, TableReplicationPhaseType};
