//! Store abstractions for ETL where state and schema are maintained.
//!
//! This module provides storage traits and implementations for maintaining ETL
//! pipeline state across restarts. It includes state tracking for replication
//! progress, versioned table schemas, destination table metadata, and
//! synchronization status.
//!
//! Storage is divided into focused capability modules:
//! - [`StateStore`] - Replication progress and table synchronization states
//! - [`SchemaStore`] - Database schema information, versioned schema storage,
//!   and obsolete schema pruning
//! - [`SharedStateStore`], [`DestinationStore`], and [`PipelineStore`] - Named
//!   facade traits for common store capability sets
//! - [`TableStateLifecycleStore`] - Table lifecycle operations that span both
//!   stores
//!
//! [`MemoryStore`] and [`PostgresStore`] provide combined implementations that
//! handle both state and schema storage in unified systems.

mod both;
mod capabilities;
mod lifecycle;
mod schema;
mod state;

pub use both::{memory::MemoryStore, postgres::PostgresStore};
pub use capabilities::{DestinationStore, PipelineStore, SharedStateStore};
pub use lifecycle::{TableStateLifecycleStore, TableStateOperation};
pub(crate) use schema::TableSchemaSnapshots;
pub use schema::{SchemaStore, TableSchemaRetention};
pub(crate) use state::{DestinationTablesMetadata, DestinationWriteStreamStates};
pub use state::{StateStore, TableStates};

pub use crate::replication::{
    WorkerType,
    state::{TableRetryPolicy, TableState, TableStateType},
};
