mod catalog;
mod client;
mod core;
mod encoding;
mod error;
mod schema;
#[cfg(feature = "test-utils")]
pub mod test_utils;

#[cfg(feature = "test-utils")]
pub use core::table_name_to_iceberg_table_name;
pub use core::{DestinationNamespace, IcebergDestination, IcebergOperationType};

pub use client::IcebergClient;
pub use encoding::UNIX_EPOCH;
pub use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_SECRET_ACCESS_KEY};
