pub mod client;
mod core;
mod encoding;
mod metrics;
mod network;
mod schema;
mod sql;
#[cfg(feature = "test-utils")]
pub mod test_utils;

use etl::schema::ColumnNameMapping;

/// Column-name mapping used at every ClickHouse destination boundary.
///
/// ClickHouse quoted identifiers preserve exact source names. Table creation,
/// row writes, schema planning, and recovery must use this same mapping.
/// Changing it for existing tables requires compatibility handling or a
/// resync.
const CLICKHOUSE_COLUMN_NAME_MAPPING: ColumnNameMapping = ColumnNameMapping::Identity;

pub use core::{ClickHouseClientConfig, ClickHouseDestination, ClickHouseInserterConfig};

pub use client::ClickHouseClient;
