//! PostgreSQL logical replication protocol implementation.
//!
//! This module provides comprehensive support for PostgreSQL logical replication,
//! including replication slot management, streaming WAL changes, and coordinating
//! both initial table synchronization and continuous replication.
//!
//! The module is organized into several key components:
//! - [`client`] - PostgreSQL replication protocol client
//! - [`stream`] - WAL event streaming and parsing
//! - [`slot`] - Replication slot lifecycle management
//! - [`apply`] - Change event application to destinations
//! - [`table_sync`] - Initial table data synchronization

pub mod apply;
pub mod client;
pub mod common;
pub mod slot;
pub mod stream;
pub mod table_sync;
