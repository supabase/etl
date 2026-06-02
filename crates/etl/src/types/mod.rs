//! Common types used throughout the ETL system.
//!
//! Re-exports core data types, event types, and schema definitions used across
//! the ETL pipeline. Includes Postgres-specific types, replication events, and
//! table structures.

mod arrow_batch;
mod cell;
mod event;
mod pipeline;
mod sized;
mod table_row;

pub use arrow_batch::{
    ChangeArrowBatch, ChangeKind, RowImage, StreamBatch, TableArrowBatch, TableChangeSet,
    TruncateBatch,
};
// Re-exports.
pub use cell::{ArrayCell, ArrayCellNonOptional, Cell, CellNonOptional};
pub use etl_postgres::types::*;
pub use event::{
    BeginEvent, CommitEvent, DeleteEvent, Event, EventSequenceKey, EventType, InsertEvent,
    RelationEvent, TruncateEvent, UpdateEvent,
};
pub use pipeline::PipelineId;
pub use sized::SizeHint;
pub use table_row::{OldTableRow, PartialTableRow, TableRow, UpdatedTableRow};
pub use tokio_postgres::types::*;

pub use crate::conversions::{PgNumeric, Sign};
