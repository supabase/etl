mod batches;
mod client;
mod config;
mod core;
mod encoding;
mod external_maintenance;
mod inline_size;
mod metrics;
mod schema;
mod sql;

use std::fmt;

use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    schema::TableName,
};
use serde::{Deserialize, Serialize};

/// The DuckDB catalog alias used in every `lake.<table>` qualified name.
pub(super) const LAKE_CATALOG: &str = "lake";

/// A table reference inside the DuckLake catalog.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct DuckLakeTableName {
    schema: String,
    table: String,
}

impl DuckLakeTableName {
    /// Creates a DuckLake table reference from explicit schema and table names.
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self { schema: schema.into(), table: table.into() }
    }

    /// Creates a DuckLake table reference that mirrors a source Postgres table.
    pub fn from_source(table_name: &TableName) -> Self {
        Self::new(table_name.schema.clone(), table_name.name.clone())
    }

    /// Returns the DuckLake schema name.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the DuckLake table name.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Returns a stable ID for logs, metrics, and replay marker rows.
    pub fn id(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// Serializes this reference for durable destination metadata.
    pub fn to_metadata_id(&self) -> EtlResult<String> {
        serde_json::to_string(self).map_err(|source| {
            etl_error!(
                ErrorKind::InvalidState,
                "DuckLake destination table metadata serialization failed",
                source: source
            )
        })
    }

    /// Parses a durable destination metadata table reference.
    pub(super) fn from_metadata_id(value: &str) -> EtlResult<Self> {
        serde_json::from_str(value).map_err(|source| {
            etl_error!(
                ErrorKind::InvalidState,
                "DuckLake destination table metadata is invalid",
                format!("destination_table_id={value}"),
                source: source
            )
        })
    }

    /// Returns whether this is an internal ETL helper table.
    pub(super) fn is_internal_helper(&self) -> bool {
        self.table.starts_with("__etl_")
    }
}

impl fmt::Display for DuckLakeTableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.id())
    }
}

/// Attach-level DuckLake data inlining limit for ETL-managed connections.
///
/// This applies to every DuckDB connection in the destination pool so small
/// writes inline into the DuckLake metadata first and can later be
/// materialized to Parquet by an external maintenance job.
const ATTACH_DATA_INLINING_ROW_LIMIT: u64 = 1_000_000;

pub use core::{
    DuckLakeDestination, DuckLakeExternalMaintenanceConfig, DuckLakeExternalMaintenancePause,
    DuckLakeMaintenanceMode, table_name_to_ducklake_table_name,
};
#[cfg(feature = "test-utils")]
pub use core::{
    arm_pause_next_streaming_write_for_tests, release_paused_streaming_write_for_tests,
    reset_paused_streaming_write_for_tests,
};

#[cfg(feature = "test-utils")]
pub use batches::{
    arm_fail_after_atomic_batch_commit_once_for_tests,
    arm_fail_after_copy_batch_commit_once_for_tests, ducklake_staging_table_creations_for_tests,
    reset_ducklake_test_hooks,
};
pub use config::S3Config;
pub use etl_maintenance::ducklake::{
    CleanupOldFilesMaintenanceConfig, DuckLakeMaintenanceConfig, DuckLakeMaintenanceOutcome,
    ExpireSnapshotsMaintenanceConfig, InlineFlushMaintenanceConfig,
    MergeAdjacentFilesMaintenanceConfig, RewriteDataFilesMaintenanceConfig, run_maintenance_once,
};
pub use external_maintenance::{
    ExternalMaintenanceOperationHistory, ExternalMaintenanceOperationPolicy,
    ExternalMaintenanceOperationRequest, ExternalMaintenanceOperationRun,
    ExternalMaintenanceOperations, ExternalMaintenancePause, ExternalMaintenancePausePolicy,
    ExternalMaintenanceReplicatorState, ExternalMaintenanceReplicatorStatus,
    ExternalMaintenanceRequestOutcome, ExternalMaintenanceRun, ExternalMaintenanceState,
    ExternalMaintenanceStore, ExternalMaintenanceWatcherConfig, PostgresExternalMaintenanceStore,
    run_external_maintenance_watcher,
};
