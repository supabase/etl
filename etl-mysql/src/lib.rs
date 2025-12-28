//! MySQL database connection utilities for all crates.
//!
//! This crate provides database connection options and utilities for working with MySQL.
//! It supports the [`sqlx`] crate through feature flags.

#[cfg(feature = "replication")]
pub mod replication;
#[cfg(feature = "sqlx")]
pub mod sqlx;
#[cfg(feature = "tokio")]
pub mod tokio;
pub mod types;
pub mod version;
