//! In-memory secondary index for efficient row lookups.
//!
//! This module provides a secondary index that maps primary keys to row locations,
//! enabling O(1) lookups for UPDATE and DELETE operations without scanning the
//! entire table.

mod primary_key;
mod row_location;
mod secondary_index;

pub use primary_key::PrimaryKey;
pub use row_location::RowLocation;
pub use secondary_index::{IndexStats, SecondaryIndex};
