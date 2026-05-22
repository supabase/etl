//! Store abstractions for ETL where state and schema are maintained.
//!
//! This module provides storage traits and implementations for maintaining ETL
//! pipeline state across restarts. It includes state tracking for replication
//! progress, versioned table schemas, destination table metadata, and
//! synchronization status.
//!
//! Storage is divided into three main categories:
//! - [`state`] - Replication progress and table synchronization states
//! - [`schema`] - Database schema information, versioned schema storage, and
//!   obsolete schema pruning
//! - [`destination`] - Combined store capabilities commonly required by
//!   destination implementations
//! - [`lifecycle`] - Table lifecycle operations that span both stores
//! - [`pipeline`] - Combined store capabilities required by the pipeline
//!
//! The [`both`] module provides combined implementations that handle both
//! state and schema storage in unified systems.

pub mod both;
pub mod destination;
pub mod lifecycle;
pub mod pipeline;
pub mod schema;
pub mod state;

pub use both::{memory::MemoryStore, postgres::PostgresStore};
pub use destination::DestinationStore;
pub use lifecycle::TableLifecycleStore;
pub use pipeline::PipelineStore;
pub use schema::SchemaStore;
pub use state::{StateStore, TableReplicationStates};
