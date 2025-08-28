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

// Re-exports.
// TODO: we might want to restructure `etl_postgres` to have better modules.
pub use etl_postgres::schema::*;
pub use etl_postgres::time::*;
pub use etl_postgres::types::*;
pub use tokio_postgres::types::*;
