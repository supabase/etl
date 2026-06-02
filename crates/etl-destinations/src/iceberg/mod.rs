//! Experimental Apache Iceberg destination.
//!
//! This module is unmaintained experimental code. It is not production
//! supported, has not been widely tested, and should only be used at your own
//! risk. Prefer the maintained destination modules for production pipelines.
//!
//! The destination writes materialized Iceberg format v2 tables rather than
//! append-only changelog tables. Table-copy rows are appended to data files.
//! Streaming inserts are written as replay-safe upserts, updates delete the old
//! primary-key identity and write the new row, deletes write equality-delete
//! files, and truncates drop and recreate the table. Simple add, drop, and
//! rename column changes are supported; full DDL, positional deletes, and
//! delete vectors are not. CDC requires all source primary-key columns to be
//! replicated and replica identity to be primary-key-equivalent or full.
//!
//! Iceberg row-level CDC produces data files, equality-delete files, manifests,
//! snapshots, and metadata files over time. This module does not run
//! compaction, snapshot expiration, manifest rewrites, or orphan-file cleanup.

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
pub use core::{DestinationNamespace, IcebergDestination};

pub use client::IcebergClient;
pub use encoding::UNIX_EPOCH;
pub use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_SECRET_ACCESS_KEY};
