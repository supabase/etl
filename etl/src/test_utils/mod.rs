//! Common utilities and helpers for testing PostgreSQL replication functionality.
//!
//! This module provides shared testing infrastructure including database management,
//! pipeline testing utilities, destination testing helpers, and table manipulation utilities.
//! It also includes common testing patterns like waiting for conditions to be met.
pub mod database;
pub mod event;
pub mod materialize;
pub mod notify;
pub mod pipeline;
pub mod table;
pub mod test_destination_wrapper;
pub mod test_schema;
