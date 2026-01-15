//! ETL Iceberg Changelog Merger library.
//!
//! This library implements a secondary index structure stored in Puffin files that maps
//! primary key hashes to file locations for efficient deduplication during merge.
//!
//! ## Core Components
//!
//! - [`index`]: Puffin-based secondary index using `splitmix64` hash function
//! - [`merge`]: Merge and compaction logic for changelog tables
//! - [`config`]: Configuration management for the merger service
//! - [`scheduler`]: Scheduled merge operations
//!
//! ## Architecture
//!
//! The merger uses a two-tier strategy optimized for S3 storage:
//!
//! 1. **Persistent Storage**: Index data is stored as Parquet in Puffin files on S3
//! 2. **Memory Cache**: Index entries are cached in-memory for fast lookups
//!
//! ## Example
//!
//! ```ignore
//! use etl_iceberg_merger::index::{PuffinIndex, splitmix64, compute_pk_hash};
//!
//! // Create a new index
//! let mut index = PuffinIndex::new(64);
//!
//! // Insert entries (typically from scanning Parquet files)
//! let pk_hash = splitmix64(12345);
//! index.insert(pk_hash, "s3://bucket/file.parquet".to_string(), 0);
//!
//! // Lookup entries
//! if let Some(location) = index.get(pk_hash) {
//!     println!("Found at {}:{}", location.file_path, location.row_offset);
//! }
//!
//! // Batch lookup for efficiency
//! let hashes = vec![splitmix64(100), splitmix64(200), splitmix64(300)];
//! let results = index.search_values(&hashes);
//! ```

pub mod config;
pub mod index;
pub mod merge;
pub mod scheduler;
