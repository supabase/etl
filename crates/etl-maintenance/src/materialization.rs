use std::error::Error;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;

use crate::coordination::{ExternalMaintenanceOperationPolicy, PostgresExternalMaintenanceStore};

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
            max_compacted_files: 32,
            max_tables_per_run: 8,
            target_file_size: "10MB".to_owned(),
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
    pub tenant_id: String,
    pub pipeline_id: i64,
    pub replicator_id: i64,
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
#[derive(Debug, Clone)]
pub struct PostgresMaintenanceMaterializer {
    pool: PgPool,
}

impl PostgresMaintenanceMaterializer {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn store(&self, pipeline_id: i64) -> PostgresExternalMaintenanceStore {
        PostgresExternalMaintenanceStore::new(pipeline_id, self.pool.clone())
    }
}

#[async_trait]
impl MaintenanceMaterializer for PostgresMaintenanceMaterializer {
    async fn reconcile_ducklake_maintenance(
        &self,
        input: DuckLakeMaintenanceMaterialization,
    ) -> Result<(), MaintenanceMaterializationError> {
        let store = self.store(input.identity.pipeline_id);
        store.ensure_schema().await.map_err(MaintenanceMaterializationError::postgres)?;
        store
            .ensure_pipeline_state(input.policy.operation_policy)
            .await
            .map_err(MaintenanceMaterializationError::postgres)?;

        Ok(())
    }

    async fn delete_ducklake_maintenance(
        &self,
        identity: MaintenanceIdentity,
    ) -> Result<(), MaintenanceMaterializationError> {
        let store = self.store(identity.pipeline_id);
        store.ensure_schema().await.map_err(MaintenanceMaterializationError::postgres)?;
        store.delete_pipeline_state().await.map_err(MaintenanceMaterializationError::postgres)?;

        Ok(())
    }
}
