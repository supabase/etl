//! Testing utilities for PostgreSQL logical replication systems.
//!
//! Provides a complete testing framework for complex ETL scenarios involving PostgreSQL logical replication,
//! multiple workers, and various destination systems. Handles test database setup, replication slot management,
//! worker lifecycle coordination, and data consistency validation.

pub mod database;
pub mod event;
pub mod materialize;
pub mod notify;
pub mod pipeline;
pub mod table;
pub mod test_destination_wrapper;
pub mod test_schema;
