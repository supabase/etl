//! DuckLake maintenance runner.

mod compatibility;
mod duckdb_error;
mod rewrite;
mod runner;

pub use compatibility::duckdb_version_check_sql;
pub use duckdb_error::{RedactedDuckDbError, redact_credentials};
pub use rewrite::DataFileRewriteReason;
pub use runner::{
    CleanupOldFilesMaintenanceConfig, DuckLakeMaintenanceConfig, DuckLakeMaintenanceOutcome,
    ExpireSnapshotsMaintenanceConfig, InlineFlushMaintenanceConfig,
    MergeAdjacentFilesMaintenanceConfig, RewriteDataFilesMaintenanceConfig, S3Config,
    flush_table_inlined_data, run_maintenance_once,
};
