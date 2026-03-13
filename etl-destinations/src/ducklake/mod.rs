mod config;
mod core;
mod schema;

/// Disable DuckLake data inlining for ETL-managed tables.
///
/// DuckLake 0.4 still has reproduced transaction commit bugs in the inlined
/// metadata path, especially with PostgreSQL catalogs and certain data types.
/// The ETL destination therefore forces non-inlined writes for stability.
const DATA_INLINING_ROW_LIMIT: usize = 0;

pub use config::{DuckDbLogConfig, S3Config};
pub use core::{DuckLakeDestination, table_name_to_ducklake_table_name};
#[cfg(feature = "test-utils")]
pub use core::{
    arm_fail_after_atomic_batch_commit_once_for_tests,
    arm_fail_after_copy_batch_commit_once_for_tests, reset_ducklake_test_hooks,
};
