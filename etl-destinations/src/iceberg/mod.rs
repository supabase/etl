mod catalog;
mod client;
mod destination;
mod encoding;
mod error;
mod schema;

pub use client::IcebergClient;
pub use destination::{
    DestinationNamespace, IcebergDestination, IcebergOperationType,
    table_name_to_iceberg_table_name,
};
pub use encoding::UNIX_EPOCH;
pub use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_SECRET_ACCESS_KEY};
