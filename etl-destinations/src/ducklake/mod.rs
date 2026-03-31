mod batches;
mod client;
mod config;
mod core;
mod encoding;
mod maintenance;
mod metrics;
mod schema;

/// The DuckDB catalog alias used in every `lake.<table>` qualified name.
pub(super) const LAKE_CATALOG: &str = "lake";

/// Alias for DuckLake table names.
pub(super) type DuckLakeTableName = String;

/// Attach-level DuckLake data inlining limit for ETL-managed connections.
///
/// This applies to every DuckDB connection in the destination pool so small
/// writes inline into the DuckLake metadata first and can later be
/// materialized to Parquet by the background maintenance worker.
const ATTACH_DATA_INLINING_ROW_LIMIT: u64 = 10_000;

#[cfg(feature = "test-utils")]
pub use batches::{
    arm_fail_after_atomic_batch_commit_once_for_tests,
    arm_fail_after_copy_batch_commit_once_for_tests, reset_ducklake_test_hooks,
};
pub use config::S3Config;
pub use core::{DuckLakeDestination, table_name_to_ducklake_table_name};
