//! Common types used throughout the ETL system.
//!
//! Re-exports core data types, event types, and schema definitions used across the ETL pipeline.
//! Includes Postgres-specific types, replication events, and table structures.

mod cell;
mod event;
mod pipeline;
mod table_row;

pub use cell::*;
pub use event::*;
pub use pipeline::*;
pub use table_row::*;

pub use crate::conversions::numeric::PgNumeric;

// Re-exports.
pub use etl_postgres::types::*;
pub use tokio_postgres::types::*;
