//! Database schema management and caching.
//!
//! This module provides functionality for managing PostgreSQL table schemas
//! and their mappings to destination systems. It includes efficient caching
//! mechanisms to minimize database roundtrips during replication.

mod cache;

pub use cache::SchemaCache;
