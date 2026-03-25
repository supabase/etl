mod config;
mod core;
mod metrics;
mod schema;

/// Attach-level DuckLake data inlining limit for ETL-managed connections.
///
/// This applies to every DuckDB connection in the destination pool so small
/// writes inline into the DuckLake metadata first and are then explicitly
/// flushed to Parquet after commit.
const ATTACH_DATA_INLINING_ROW_LIMIT: usize = 500; // This number has been computed from our metrics in prod

pub use config::{DuckDbLogConfig, S3Config};
pub use core::{DuckLakeDestination, table_name_to_ducklake_table_name};
#[cfg(feature = "test-utils")]
pub use core::{
    arm_fail_after_atomic_batch_commit_once_for_tests,
    arm_fail_after_atomic_batch_flush_once_for_tests,
    arm_fail_after_copy_batch_commit_once_for_tests,
    arm_fail_after_copy_batch_flush_once_for_tests, reset_ducklake_test_hooks,
};
