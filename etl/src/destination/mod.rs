//! Data destination abstractions for ETL pipelines.
//!
//! This module provides the core [`Destination`] trait and implementations for sending
//! replicated data to various target systems. Destinations receive both initial table
//! synchronization data and streaming replication events.

mod base;
pub mod memory;

pub use base::Destination;
