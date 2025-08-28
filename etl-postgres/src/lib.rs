//! Postgres database connection utilities for all crates.
//!
//! This crate provides database connection options and utilities for working with Postgres.
//! It supports both the [`sqlx`] and [`tokio-postgres`] crates through feature flags.

#[cfg(feature = "replication")]
pub mod replication;
#[cfg(feature = "sqlx")]
pub mod sqlx;
#[cfg(feature = "tokio")]
pub mod tokio;
pub mod types;
