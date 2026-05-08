//! Postgres database connection utilities for all crates.
//!
//! This crate provides database connection options and utilities for working
//! with Postgres. It supports both the [`sqlx`] and [`tokio-postgres`] crates
//! through feature flags.

pub mod db;
pub mod lag;
pub mod slots;
#[cfg(feature = "sqlx")]
pub mod sqlx;
pub mod store;
#[cfg(feature = "tokio")]
pub mod tokio;
pub mod types;
pub mod version;
