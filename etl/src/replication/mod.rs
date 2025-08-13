//! PostgreSQL logical replication protocol implementation.
//!
//! Handles the PostgreSQL logical replication protocol including slot management,
//! streaming changes, and maintaining replication consistency.

pub mod apply;
pub mod client;
pub mod common;
pub mod stream;
pub mod table_sync;
