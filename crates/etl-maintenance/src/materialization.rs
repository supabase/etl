use std::{error::Error, fmt, num::ParseIntError};

use async_trait::async_trait;
use etl_config::shared::DEFAULT_DUCKLAKE_TARGET_FILE_SIZE;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;

use crate::coordination::{
    ExternalMaintenanceOperationPolicy, ExternalMaintenancePausePolicy,
    PostgresExternalMaintenanceStore,
};

/// DuckLake maintenance policy independent of the coordination backend.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DuckLakeMaintenancePolicy {
    /// Minimum time between maintenance runs, in seconds.
    pub min_interval_seconds: u64,
    /// Maximum time replication may be paused for one maintenance run, in
    /// seconds.
    pub max_pause_seconds: u64,
    /// Minimum inlined bytes required before inline flush runs.
    pub min_inlined_bytes: u64,
    /// Maximum number of adjacent files compacted by one merge operation.
    pub max_compacted_files: u32,
    /// Maximum number of tables processed by each operation in one run.
    pub max_tables_per_run: u32,
    /// DuckLake target file size used for compaction.
    pub target_file_size: String,
    /// Deleted-row fraction that triggers data file rewrite.
    pub delete_threshold: f64,
    /// Minimum active data files required before data file rewrite runs.
    pub min_active_data_files: i64,
    /// CPU request for maintenance jobs, in millicores.
    pub cpu_request_millicores: u32,
    /// Memory request for maintenance jobs, in MiB.
    pub memory_request_mib: u32,
    /// Maximum runtime for one maintenance job, in seconds.
    pub active_deadline_seconds: i64,
    /// Backend-neutral operation enablement policy.
    pub operation_policy: ExternalMaintenanceOperationPolicy,
}

impl Default for DuckLakeMaintenancePolicy {
    fn default() -> Self {
        Self {
            min_interval_seconds: 3600,
            max_pause_seconds: 2700,
            min_inlined_bytes: 10_000_000,
            max_compacted_files: 40,
            max_tables_per_run: 8,
            target_file_size: DEFAULT_DUCKLAKE_TARGET_FILE_SIZE.to_owned(),
            delete_threshold: 0.5,
            min_active_data_files: 40,
            cpu_request_millicores: 1000,
            memory_request_mib: 1024,
            active_deadline_seconds: 1800,
            operation_policy: ExternalMaintenanceOperationPolicy::default(),
        }
    }
}

/// Pipeline identifier accepted during the migration from integers to strings.
///
/// Serialization preserves the original representation so legacy integer
/// identifiers remain compatible with integer-only maintenance CRD schemas.
/// Callers must keep writing integers until those schemas accept strings.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum MaintenancePipelineId {
    /// Legacy signed 64-bit pipeline identifier.
    Integer(i64),
    /// String pipeline identifier, preserved without normalization.
    String(String),
}

impl From<i64> for MaintenancePipelineId {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<String> for MaintenancePipelineId {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for MaintenancePipelineId {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl fmt::Display for MaintenancePipelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(value) => value.fmt(f),
            Self::String(value) => f.write_str(value),
        }
    }
}

/// Converts identifiers for backends that still require an integer key.
impl TryFrom<&MaintenancePipelineId> for i64 {
    type Error = ParseIntError;

    fn try_from(value: &MaintenancePipelineId) -> Result<Self, Self::Error> {
        match value {
            MaintenancePipelineId::Integer(value) => Ok(*value),
            MaintenancePipelineId::String(value) => value.parse(),
        }
    }
}

/// Stable identity for one pipeline's external maintenance runtime state.
#[derive(Debug, Clone)]
pub struct MaintenanceIdentity {
    /// Tenant/project identifier.
    pub tenant_id: String,
    /// Pipeline identifier in its legacy integer or new string representation.
    pub pipeline_id: MaintenancePipelineId,
    /// Replicator identifier used in runtime resource names.
    pub replicator_id: i64,
    /// Deployment-specific prefix for runtime resources.
    pub resource_prefix: String,
}

/// Deployment-specific references required by the external maintenance runner.
#[derive(Debug, Clone)]
pub struct MaintenanceRuntimeRefs {
    pub replicator_image: String,
}

/// Backend-neutral materialization input for DuckLake external maintenance.
#[derive(Debug, Clone)]
pub struct DuckLakeMaintenanceMaterialization {
    pub identity: MaintenanceIdentity,
    pub policy: DuckLakeMaintenancePolicy,
    pub runtime_refs: MaintenanceRuntimeRefs,
}

/// Configured external maintenance backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceMaterializerKind {
    Kubernetes,
    Postgres,
    Disabled,
}

/// Errors raised while creating or deleting external maintenance runtime state.
#[derive(Debug, Error)]
pub enum MaintenanceMaterializationError {
    /// A string identifier cannot address the existing Postgres bigint key.
    #[error("Postgres maintenance requires a signed 64-bit integer pipeline ID")]
    InvalidPostgresPipelineId {
        /// Integer conversion failure without the original identifier value.
        #[source]
        source: ParseIntError,
    },

    #[error("maintenance backend `{0:?}` is not configured in this deployment")]
    BackendNotConfigured(MaintenanceMaterializerKind),

    #[error("{backend:?} maintenance materialization failed")]
    Backend {
        backend: MaintenanceMaterializerKind,
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
}

impl MaintenanceMaterializationError {
    /// Wraps a backend-specific materialization error.
    pub fn backend<E>(backend: MaintenanceMaterializerKind, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Backend { backend, source: Box::new(source) }
    }

    /// Wraps a Kubernetes materialization error.
    pub fn kubernetes<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::backend(MaintenanceMaterializerKind::Kubernetes, source)
    }

    /// Wraps a Postgres materialization error.
    fn postgres(source: etl::error::EtlError) -> Self {
        Self::backend(MaintenanceMaterializerKind::Postgres, source)
    }
}

/// Deployment-side abstraction for external maintenance runtime state.
#[async_trait]
pub trait MaintenanceMaterializer: Send + Sync {
    async fn reconcile_ducklake_maintenance(
        &self,
        input: DuckLakeMaintenanceMaterialization,
    ) -> Result<(), MaintenanceMaterializationError>;

    async fn delete_ducklake_maintenance(
        &self,
        identity: MaintenanceIdentity,
    ) -> Result<(), MaintenanceMaterializationError>;
}

/// Explicit no-op materializer for deployments with no external maintenance
/// backend configured.
#[derive(Debug, Default)]
pub struct DisabledMaintenanceMaterializer;

#[async_trait]
impl MaintenanceMaterializer for DisabledMaintenanceMaterializer {
    async fn reconcile_ducklake_maintenance(
        &self,
        _input: DuckLakeMaintenanceMaterialization,
    ) -> Result<(), MaintenanceMaterializationError> {
        Ok(())
    }

    async fn delete_ducklake_maintenance(
        &self,
        _identity: MaintenanceIdentity,
    ) -> Result<(), MaintenanceMaterializationError> {
        Ok(())
    }
}

/// Postgres materializer for deployments that coordinate maintenance without a
/// Kubernetes CRD.
///
/// The existing bigint key accepts integer IDs and numeric strings. Opaque
/// string IDs require a separate storage migration.
#[derive(Debug, Clone)]
pub struct PostgresMaintenanceMaterializer {
    pool: PgPool,
}

impl PostgresMaintenanceMaterializer {
    /// Creates a materializer using the existing Postgres maintenance schema.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Converts the identifier before accessing the legacy bigint-backed store.
    fn store(
        &self,
        pipeline_id: &MaintenancePipelineId,
    ) -> Result<PostgresExternalMaintenanceStore, MaintenanceMaterializationError> {
        let pipeline_id = i64::try_from(pipeline_id).map_err(|source| {
            MaintenanceMaterializationError::InvalidPostgresPipelineId { source }
        })?;
        Ok(PostgresExternalMaintenanceStore::new(pipeline_id, self.pool.clone()))
    }
}

#[async_trait]
impl MaintenanceMaterializer for PostgresMaintenanceMaterializer {
    async fn reconcile_ducklake_maintenance(
        &self,
        input: DuckLakeMaintenanceMaterialization,
    ) -> Result<(), MaintenanceMaterializationError> {
        let store = self.store(&input.identity.pipeline_id)?;
        store.ensure_schema().await.map_err(MaintenanceMaterializationError::postgres)?;
        store
            .ensure_pipeline_state(
                ExternalMaintenancePausePolicy {
                    max_duration_seconds: input.policy.max_pause_seconds,
                },
                input.policy.operation_policy,
            )
            .await
            .map_err(MaintenanceMaterializationError::postgres)?;

        Ok(())
    }

    async fn delete_ducklake_maintenance(
        &self,
        identity: MaintenanceIdentity,
    ) -> Result<(), MaintenanceMaterializationError> {
        let store = self.store(&identity.pipeline_id)?;
        store.ensure_schema().await.map_err(MaintenanceMaterializationError::postgres)?;
        store.delete_pipeline_state().await.map_err(MaintenanceMaterializationError::postgres)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use serde_json::json;
    use sqlx::postgres::PgPoolOptions;

    use crate::{
        DuckLakeMaintenanceMaterialization, DuckLakeMaintenancePolicy, MaintenanceIdentity,
        MaintenanceMaterializationError, MaintenanceMaterializer, MaintenancePipelineId,
        MaintenanceRuntimeRefs, PostgresMaintenanceMaterializer,
    };

    /// Integer identifiers retain their numeric wire representation and range.
    #[test]
    fn pipeline_id_round_trips_integers() {
        for value in [i64::MIN, -1, 0, 42, i64::MAX] {
            let pipeline_id: MaintenancePipelineId = serde_json::from_value(json!(value)).unwrap();

            assert_eq!(pipeline_id, MaintenancePipelineId::from(value));
            assert_eq!(serde_json::to_value(&pipeline_id).unwrap(), json!(value));
            assert_eq!(pipeline_id.to_string(), value.to_string());
            assert_eq!(i64::try_from(&pipeline_id).unwrap(), value);
        }
    }

    /// Strings retain their exact contents, including numeric-looking IDs.
    #[test]
    fn pipeline_id_round_trips_strings() {
        for value in ["42", "0042", "pipeline-example", "9223372036854775808", ""] {
            let pipeline_id: MaintenancePipelineId = serde_json::from_value(json!(value)).unwrap();

            assert_eq!(pipeline_id, MaintenancePipelineId::from(value.to_owned()));
            assert_eq!(pipeline_id, MaintenancePipelineId::from(value));
            assert_eq!(serde_json::to_value(&pipeline_id).unwrap(), json!(value));
            assert_eq!(pipeline_id.to_string(), value);
        }
    }

    /// Other JSON shapes and numbers outside the signed range are rejected.
    #[test]
    fn pipeline_id_rejects_invalid_wire_types() {
        for json in [
            "null",
            "true",
            "42.0",
            "42.5",
            "[]",
            "{}",
            "9223372036854775808",
            "-9223372036854775809",
        ] {
            assert!(serde_json::from_str::<MaintenancePipelineId>(json).is_err());
        }
    }

    /// Numeric strings can still address the existing Postgres bigint keys.
    #[test]
    fn pipeline_id_converts_numeric_strings_for_legacy_backends() {
        for value in [i64::MIN, -1, 0, 42, i64::MAX] {
            let pipeline_id = MaintenancePipelineId::from(value.to_string());

            assert_eq!(i64::try_from(&pipeline_id).unwrap(), value);
        }
    }

    /// Incompatible strings return conversion errors without panicking.
    #[test]
    fn pipeline_id_rejects_invalid_legacy_integer_conversion() {
        for value in [
            "",
            "pipeline-example",
            "４２",
            " 42",
            "42.0",
            "9223372036854775808",
            "-9223372036854775809",
        ] {
            let pipeline_id = MaintenancePipelineId::from(value);

            assert!(i64::try_from(&pipeline_id).is_err());
        }
    }

    /// Unsupported Postgres IDs fail before migrations, writes, or deletes.
    #[tokio::test]
    async fn postgres_materializer_rejects_opaque_ids_before_database_access() {
        let pool =
            PgPoolOptions::new().connect_lazy("postgres://postgres@127.0.0.1/postgres").unwrap();
        // A closed pool makes any accidental database access fail immediately.
        pool.close().await;
        let materializer = PostgresMaintenanceMaterializer::new(pool);
        let identity = MaintenanceIdentity {
            tenant_id: "tenant-example".to_owned(),
            pipeline_id: "pipeline-example".into(),
            replicator_id: 1,
            resource_prefix: "replicator".to_owned(),
        };
        let input = DuckLakeMaintenanceMaterialization {
            identity: identity.clone(),
            policy: DuckLakeMaintenancePolicy::default(),
            runtime_refs: MaintenanceRuntimeRefs {
                replicator_image: "etl-replicator:test".to_owned(),
            },
        };

        let reconcile_error = materializer.reconcile_ducklake_maintenance(input).await.unwrap_err();
        let delete_error = materializer.delete_ducklake_maintenance(identity).await.unwrap_err();

        for error in [reconcile_error, delete_error] {
            assert!(matches!(
                error,
                MaintenanceMaterializationError::InvalidPostgresPipelineId { .. }
            ));
            assert!(error.source().is_some());
        }
    }
}
