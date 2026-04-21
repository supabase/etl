//! Store abstractions for ETL where state and schema are maintained.
//!
//! This module provides storage traits and implementations for maintaining ETL
//! pipeline state across restarts. It includes state tracking for replication
//! progress, table schemas, and synchronization status.
//!
//! Storage is divided into three main categories:
//! - [`state`] - Replication progress and table synchronization states
//! - [`schema`] - Database schema information and versioned schema storage
//! - [`cleanup`] - Cleanup methods that span both stores
//!
//! The [`both`] module provides combined implementations that handle both
//! state and schema storage in unified systems.

pub mod both;
pub mod cleanup;
pub mod schema;
pub mod state;

pub use both::{memory::MemoryStore, postgres::PostgresStore};
pub use cleanup::CleanupStore;
pub use schema::SchemaStore;
pub use state::{StateStore, TableReplicationStates};
