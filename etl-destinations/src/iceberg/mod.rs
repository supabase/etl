mod catalog;
mod client;
mod core;
pub mod deletion_vector;
mod encoding;
mod error;
mod manifest;
pub mod proxy_structs;
mod schema;
#[cfg(feature = "test-utils")]
pub mod test_utils;

pub use client::{DataFileInfo, IcebergClient, InsertFilesResult};
pub use core::{
    DestinationNamespace, IcebergDestination, IcebergOperationType,
    table_name_to_iceberg_table_name,
};
pub use deletion_vector::{
    DELETION_VECTOR_CARDINALITY, DELETION_VECTOR_MAGIC, DELETION_VECTOR_REFERENCED_DATA_FILE,
    DeletionVector, DeletionVectorDescriptor, commit_deletion_vectors,
    create_deletion_vector_data_file, get_data_file_paths, write_deletion_vector,
};
pub use encoding::UNIX_EPOCH;
pub use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_SECRET_ACCESS_KEY};
pub use proxy_structs::{DataFileProxy, PuffinBlobMetadataProxy, get_puffin_metadata_and_close};
