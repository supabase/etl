//! Postgres logical replication protocol implementation.
//!
//! Handles the Postgres logical replication protocol including slot management,
//! streaming changes, and maintaining replication consistency.

pub mod apply;
pub mod client;
pub mod common;
pub mod stream;
pub mod table_sync;
