//! ETL Merger - Merges CDC changelog tables into mirror tables.
//!
//! This crate provides functionality to read CDC events from Iceberg changelog
//! tables and merge them into mirror tables that represent exact replicas of
//! the source Postgres tables (without CDC metadata columns).
//!
//! # Architecture
//!
//! The merger maintains an in-memory secondary index on primary keys to enable
//! efficient row lookups for UPDATE and DELETE operations. This avoids full
//! table scans when applying changes.
//!
//! # Usage
//!
//! ```rust,no_run
//! use etl_merger::{Merger, MergerConfig};
//! use etl_destinations::iceberg::IcebergClient;
//! use etl_postgres::types::ColumnSchema;
//! use std::collections::HashMap;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = IcebergClient::new_with_rest_catalog(
//!         "http://localhost:8182/catalog".to_string(),
//!         "my_warehouse".to_string(),
//!         HashMap::new(),
//!     ).await?;
//!
//!     let config = MergerConfig::new(
//!         "default".to_string(),  // changelog namespace
//!         "mirror".to_string(),   // mirror namespace
//!     );
//!
//!     // Define mirror schema (same as source Postgres table)
//!     let mirror_schema = vec![
//!         // ... column schemas ...
//!     ];
//!
//!     let mut merger = Merger::new(
//!         client.clone(),
//!         "users_changelog",
//!         "users",
//!         mirror_schema,
//!         vec![0],  // PK column index
//!         config,
//!     ).await?;
//!
//!     // Build index from existing mirror table
//!     merger.build_index_from_mirror(&client).await?;
//!
//!     let summary = merger.merge_until_caught_up().await?;
//!     println!("Processed {} events", summary.events_processed);
//!
//!     Ok(())
//! }
//! ```

mod changelog_reader;
mod config;
mod error;
pub mod index;
mod merger;
mod mirror_writer;
mod progress;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub use changelog_reader::{
    CDC_OPERATION_COLUMN_NAME, CdcOperation, ChangelogBatch, ChangelogEntry, ChangelogReader,
    SEQUENCE_NUMBER_COLUMN_NAME,
};
pub use config::MergerConfig;
pub use error::{MergerError, MergerResult};
pub use index::{IndexStats, PrimaryKey, RowLocation, SecondaryIndex};
pub use merger::{MergeBatchResult, MergeSummary, Merger};
pub use mirror_writer::{InsertResult, MirrorWriter};
pub use progress::MergeProgress;
