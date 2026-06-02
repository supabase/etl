//! State management for replication progress tracking.
//!
//! Defines state types and enums used to track table states and
//! pipeline progress across restarts and worker coordination.

pub mod destination_table_metadata;
mod table_error;
mod table_retry_policy;
mod table_state;

pub use destination_table_metadata::{
    AppliedDestinationTableMetadata, DestinationTableMetadata, DestinationTableSchemaStatus,
};
pub use table_error::TableError;
pub use table_retry_policy::TableRetryPolicy;
pub use table_state::{TableState, TableStateType};
