//! PostgreSQL database connection utilities for all crates.
//!
//! This crate provides database connection options and utilities for working with PostgreSQL.
//! It supports both the [`sqlx`] and [`tokio-postgres`] crates through feature flags.

#[cfg(feature = "replication")]
pub mod replication;
pub mod schema;
#[cfg(feature = "sqlx")]
pub mod sqlx;
pub mod time;
#[cfg(feature = "tokio")]
pub mod tokio;
pub mod types;
