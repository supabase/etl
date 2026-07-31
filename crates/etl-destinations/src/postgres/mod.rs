//! Postgres destination with automatic schema management.
//!
//! Replicates Postgres tables into another Postgres database using
//! current-state UPSERT tables (no CDC meta columns). Destination schemas and
//! tables are created on first write, and supported schema changes are applied
//! from [`etl::event::Event::Relation`] events.

mod client;
mod core;
mod encoding;
mod schema;
mod sql;
#[cfg(feature = "test-utils")]
pub mod test_utils;

pub use core::PostgresDestination;
