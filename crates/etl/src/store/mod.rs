//! Store abstractions for ETL where state and schema are maintained.
//!
//! This module provides storage traits and implementations for maintaining ETL
//! pipeline state across restarts. It includes state tracking for replication
//! checkpoints, versioned table schemas, destination table metadata, and
//! synchronization status.
//!
//! Storage is divided into focused capability modules:
//! - [`CachedStore`] - Complete cache preparation and refresh
//! - [`StateStore`] - Persisted checkpoints and table synchronization states
//! - [`SchemaStore`] - Database schema information, versioned schema storage,
//!   and obsolete schema pruning
//! - [`SharedStateStore`], [`DestinationStore`], and [`PipelineStore`] - Named
//!   facade traits for common store capability sets
//! - [`TableStateLifecycleStore`] - Table lifecycle operations that span both
//!   stores
//!
//! [`MemoryStore`] and [`PostgresStore`] provide combined implementations that
//! handle both state and schema storage in unified systems.
//!
//! Use one store and its clones per pipeline so workers share coherent caches.
//! External changes to ETL state or destination data are unsupported. Stores
//! own caching and query batching: startup loads state, metadata, checkpoints,
//! and all retained schemas; getters normally read memory. Writes update caches
//! after persistence succeeds, and durable-checkpoint pruning removes obsolete
//! schemas from both storage and cache. Pipeline startup calls
//! [`CachedStore::load_cache`] before starting workers.
//! [`PostgresStore`] starts with an unusable cache and marks it unusable before
//! mutations. A failed or cancelled mutation leaves it unusable, so the next
//! accessor reloads every cache together. Reloads wait for earlier database
//! transactions to settle and publish only after every load succeeds. Failed
//! reloads return an error and remain eligible for retry.

mod both;
mod cache;
mod capabilities;
mod lifecycle;
mod schema;
mod state;

pub use both::{memory::MemoryStore, postgres::PostgresStore};
pub use cache::CachedStore;
pub use capabilities::{DestinationStore, PipelineStore, SharedStateStore};
pub use lifecycle::{TableStateLifecycleStore, TableStateOperation};
pub use schema::SchemaStore;
pub(crate) use schema::TableSchemaSnapshots;
pub(crate) use state::DestinationTablesMetadata;
pub use state::{StateStore, TableStates};

pub use crate::replication::{
    WorkerType,
    state::{StoredTableDecodingState, TableRetryPolicy, TableState, TableStateType},
};
