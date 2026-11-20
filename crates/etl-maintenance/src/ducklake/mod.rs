//! DuckLake maintenance runner.

mod compatibility;
mod duckdb_error;
mod metadata;
mod metrics;
mod namespace;
mod rewrite;
mod runner;
mod writer_options;

pub use compatibility::duckdb_version_check_sql;
pub use duckdb_error::{RedactedDuckDbError, redact_credentials};
pub use metadata::DuckLakeMetadataPool;
pub use namespace::{namespace_version_check_sql, namespaced_tables_query, table_namespace_filter};
pub use rewrite::DataFileRewriteReason;
pub use runner::{
    CleanupOldFilesMaintenanceConfig, DuckLakeMaintenanceConfig, DuckLakeMaintenanceOutcome,
    ExpireSnapshotsMaintenanceConfig, InlineFlushMaintenanceConfig,
    MergeAdjacentFilesMaintenanceConfig, RewriteDataFilesMaintenanceConfig, S3Config,
    flush_table_inlined_data, pending_inline_data_bytes_query, run_maintenance_once,
};
pub use writer_options::configure_parquet_settings_sql;
