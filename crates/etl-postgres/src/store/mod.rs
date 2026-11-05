//! SQL helpers for ETL-owned metadata tables stored in Postgres.
//!
//! String identities are authoritative.

pub mod catalog;
pub mod checkpoint;
pub mod destination_table_metadata;
pub mod health;
pub mod schema;
pub mod table_state;
