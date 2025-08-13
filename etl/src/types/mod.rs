//! Common types used throughout the ETL system.
//!
//! Re-exports core data types, event types, and schema definitions used across the ETL pipeline.
//! Includes PostgreSQL-specific types, replication events, and table structures.

mod pipeline;

// TODO: properly implement types in this module and export them.
pub use crate::conversions::{
    ArrayCell, ArrayCellNonOptional, Cell, CellNonOptional, event::BeginEvent, event::CommitEvent,
    event::DeleteEvent, event::Event, event::EventType, event::InsertEvent, event::TruncateEvent,
    event::UpdateEvent, numeric::ParseNumericError, numeric::PgNumeric, table_row::TableRow,
};
pub use pipeline::*;

// Re-exports.
pub use etl_postgres::schema::{ColumnSchema, Oid, TableId, TableName, TableSchema};
pub use tokio_postgres::types::PgLsn;
pub use tokio_postgres::types::Type;
