//! Common types used throughout the ETL system.
//!
//! Re-exports core data types, event types, and schema definitions used across
//! the ETL pipeline. Includes Postgres-specific types, replication events, and
//! table structures.

mod cell;
mod event;
mod pipeline;
mod sized;
mod table_row;
mod temporal;

// Re-exports.
pub use cell::{ArrayCell, Cell};
pub use etl_postgres::types::*;
pub use event::{
    BeginEvent, CommitEvent, DeleteEvent, Event, EventSequenceKey, EventType, InsertEvent,
    RelationEvent, TruncateEvent, UpdateEvent,
};
pub use pipeline::PipelineId;
pub use sized::SizeHint;
pub use table_row::{OldTableRow, PartialTableRow, TableRow, UpdatedTableRow};
pub use temporal::{
    PgDate, PgTemporalBound, PgTemporalOutOfRange, PgTime, PgTimestamp, PgTimestampTz,
};
pub use tokio_postgres::types::*;

pub use crate::conversions::{InvalidSign, ParseNumericError, PgNumeric, Sign};
