//! Store abstractions for ETL where state and schema are maintained.
//!
//! This module provides storage traits and implementations for maintaining ETL
//! pipeline state across restarts. It includes state tracking for replication
//! progress, table schemas, and synchronization status.
//!
//! Storage is divided into two main categories:
//! - [`state`] - Replication progress and table synchronization states
//! - [`schema`] - Database schema information and table mappings
//!
//! The [`both`] module provides combined implementations that handle both
//! state and schema storage in unified systems.

pub mod both;
pub mod schema;
pub mod state;
