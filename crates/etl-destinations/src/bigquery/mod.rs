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
const BIGQUERY_COLUMN_NAME_MAPPING: ColumnNameMapping = ColumnNameMapping::AsciiLowercase;

pub use core::BigQueryDestination;
#[cfg(feature = "test-utils")]
pub use core::table_name_to_bigquery_table_id;

pub use client::{BigQueryClient, BigQueryDatasetId, BigQueryProjectId, BigQueryTableId};
