mod catalog;
mod client;
mod core;
mod encoding;
mod error;
mod schema;
#[cfg(feature = "test-utils")]
pub mod test_utils;

use etl::schema::ColumnNameMapping;

/// Column-name mapping used at every Iceberg destination boundary.
///
/// Iceberg fields preserve exact source names. Initial table creation and row
/// writes must use this same mapping. Iceberg currently rejects changed
/// relation schemas instead of executing a schema plan.
const ICEBERG_COLUMN_NAME_MAPPING: ColumnNameMapping = ColumnNameMapping::Identity;

#[cfg(feature = "test-utils")]
pub use core::table_name_to_iceberg_table_name;
pub use core::{DestinationNamespace, IcebergDestination, IcebergOperationType};

pub use client::IcebergClient;
pub use encoding::UNIX_EPOCH;
pub use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_SECRET_ACCESS_KEY};
