use std::error::Error;

use async_trait::async_trait;
use etl::{error::EtlError, pipeline::PipelineId};
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

/// Stable identity for one pipeline's external maintenance runtime state.
#[derive(Debug, Clone)]
pub struct MaintenanceIdentity {
    /// Tenant/project identifier.
    pub tenant_id: String,
    /// Pipeline identifier.
    pub pipeline_id: PipelineId,
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
    fn postgres(source: EtlError) -> Self {
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
/// Pipeline IDs address text keys without normalizing their contents.
#[derive(Debug, Clone)]
pub struct PostgresMaintenanceMaterializer {
    pool: PgPool,
}

impl PostgresMaintenanceMaterializer {
    /// Creates a materializer using the existing Postgres maintenance schema.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Converts the wire identifier to the text-backed store identity.
    fn store(&self, pipeline_id: &PipelineId) -> PostgresExternalMaintenanceStore {
        PostgresExternalMaintenanceStore::new(pipeline_id.to_string(), self.pool.clone())
    }
}

#[async_trait]
impl MaintenanceMaterializer for PostgresMaintenanceMaterializer {
    async fn reconcile_ducklake_maintenance(
        &self,
        input: DuckLakeMaintenanceMaterialization,
    ) -> Result<(), MaintenanceMaterializationError> {
        let store = self.store(&input.identity.pipeline_id);
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
        let store = self.store(&identity.pipeline_id);
        store.ensure_schema().await.map_err(MaintenanceMaterializationError::postgres)?;
        store.delete_pipeline_state().await.map_err(MaintenanceMaterializationError::postgres)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use etl::pipeline::PipelineId;
    use serde_json::json;
    use sqlx::postgres::PgPoolOptions;

    use crate::{
        DuckLakeMaintenanceMaterialization, DuckLakeMaintenancePolicy, MaintenanceIdentity,
        MaintenanceMaterializationError, MaintenanceMaterializer, MaintenanceMaterializerKind,
        MaintenanceRuntimeRefs, PostgresMaintenanceMaterializer,
    };

    /// Strings retain their exact contents, including numeric-looking IDs.
    #[test]
    fn pipeline_id_round_trips_strings() {
        for value in ["42", "0042", "+42", "-0", "pipeline-example", "9223372036854775808", ""] {
            let pipeline_id: PipelineId = serde_json::from_value(json!(value)).unwrap();

            assert_eq!(pipeline_id, PipelineId::from(value.to_owned()));
            assert_eq!(pipeline_id, PipelineId::from(value));
            assert_eq!(serde_json::to_value(&pipeline_id).unwrap(), json!(value));
            assert_eq!(pipeline_id.to_string(), value);
        }
    }

    /// Other JSON shapes and numbers outside the signed range are rejected.
    #[test]
    fn pipeline_id_rejects_invalid_wire_types() {
        for json in [
            "42",
            "-1",
            "null",
            "true",
            "42.0",
            "42.5",
            "[]",
            "{}",
            "9223372036854775808",
            "-9223372036854775809",
        ] {
            assert!(serde_json::from_str::<PipelineId>(json).is_err());
        }
    }

    /// String IDs reach the text-backed store without integer conversion.
    #[tokio::test]
    async fn postgres_materializer_routes_string_ids_to_text_store() {
        let pool =
            PgPoolOptions::new().connect_lazy("postgres://postgres@127.0.0.1/postgres").unwrap();
        // A closed pool exposes the storage boundary without a live database.
        pool.close().await;
        let materializer = PostgresMaintenanceMaterializer::new(pool);

        for value in ["pipeline-example", "042", "0042", "+42", "-042", "00", "+0", "-0"] {
            let identity = MaintenanceIdentity {
                tenant_id: "tenant-example".to_owned(),
                pipeline_id: value.into(),
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

            let reconcile_error =
                materializer.reconcile_ducklake_maintenance(input).await.unwrap_err();
            let delete_error =
                materializer.delete_ducklake_maintenance(identity).await.unwrap_err();

            for error in [reconcile_error, delete_error] {
                assert!(matches!(
                    error,
                    MaintenanceMaterializationError::Backend {
                        backend: MaintenanceMaterializerKind::Postgres,
                        ..
                    }
                ));
                assert!(error.source().is_some());
            }
        }
    }
}
