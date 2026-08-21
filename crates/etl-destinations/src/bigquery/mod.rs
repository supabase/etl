mod client;
mod core;
mod encoding;
mod metrics;
mod schema;
mod sql;
#[cfg(feature = "test-utils")]
pub mod test_utils;

use etl::schema::ColumnNameMapping;

/// Column-name mapping used at every BigQuery destination boundary.
///
/// BigQuery identifiers are canonicalized to deterministic ASCII lowercase.
/// Table creation, row writes, schema planning, and recovery must use this same
/// mapping. Changing it for existing tables requires compatibility handling or
/// a resync.
///
/// BigQuery documents column names as case-insensitive but does not specify the
/// Unicode case-folding algorithm used for identifier resolution. ASCII-only
/// folding deliberately preserves flexible Unicode names instead of guessing
/// those semantics. For example, `Name` and `name` collide locally, while `Ä`
/// and `ä` remain distinct; if BigQuery considers the latter pair equivalent,
/// table DDL rejects the schema instead of ETL silently merging two columns.
const BIGQUERY_COLUMN_NAME_MAPPING: ColumnNameMapping = ColumnNameMapping::AsciiLowercase;

pub use core::BigQueryDestination;
#[cfg(feature = "test-utils")]
pub use core::table_name_to_bigquery_table_id;

pub use client::{BigQueryClient, BigQueryDatasetId, BigQueryProjectId, BigQueryTableId};
