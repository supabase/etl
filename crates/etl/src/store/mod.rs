//! Store abstractions for ETL where state and schema are maintained.
//!
//! This module provides storage traits and implementations for maintaining ETL
//! pipeline state across restarts. It includes state tracking for replication
//! progress, versioned table schemas, destination table metadata, and
//! synchronization status.
//!
//! Storage is divided into focused capability modules:
//! - [`state`] - Replication progress and table synchronization states
//! - [`schema`] - Database schema information, versioned schema storage, and
//!   obsolete schema pruning
//! - [`capabilities`] - Named facade traits for common store capability sets
//! - [`lifecycle`] - Table lifecycle operations that span both stores
//!
//! The [`both`] module provides combined implementations that handle both
//! state and schema storage in unified systems.

pub mod both;
pub mod capabilities;
pub mod lifecycle;
pub mod schema;
pub mod state;

pub use both::{memory::MemoryStore, postgres::PostgresStore};
pub use capabilities::{DestinationStore, PipelineStore, SharedStateStore};
pub use lifecycle::{TableStateLifecycleStore, TableStateOperation};
pub use schema::SchemaStore;
pub use state::{StateStore, TableStates};
