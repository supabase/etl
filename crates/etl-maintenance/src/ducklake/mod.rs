//! DuckLake maintenance runner.

mod duckdb_error;
mod runner;

pub use duckdb_error::{RedactedDuckDbError, redact_credentials};
pub use runner::{
    CleanupOldFilesMaintenanceConfig, DuckLakeMaintenanceConfig, DuckLakeMaintenanceOutcome,
    ExpireSnapshotsMaintenanceConfig, InlineFlushMaintenanceConfig,
    MergeAdjacentFilesMaintenanceConfig, RewriteDataFilesMaintenanceConfig, S3Config,
    flush_table_inlined_data, run_maintenance_once,
};
