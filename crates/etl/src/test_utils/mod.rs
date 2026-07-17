//! Testing utilities for Postgres logical replication systems.
//!
//! Provides a complete testing framework for complex ETL scenarios involving
//! Postgres logical replication, multiple workers, and various destination
//! systems. Handles test database setup, replication slot management,
//! worker lifecycle coordination, and data consistency validation.

pub mod database;
pub mod event;
pub mod faults;
pub mod materialize;
pub mod memory_destination;
pub mod notify;
pub mod notifying_store;
pub mod pipeline;
// Gated on the feature alone (not `cfg(test)`) because the runner needs the
// optional `proptest` dependency that only the `test-utils` feature enables.
#[cfg(feature = "test-utils")]
pub mod property;
pub mod replication_stream;
pub mod schema;
pub mod test_destination_wrapper;
pub mod test_schema;
