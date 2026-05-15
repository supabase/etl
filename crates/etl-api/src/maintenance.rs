use async_trait::async_trait;
use etl_maintenance::{ExternalMaintenanceOperationPolicy, PostgresExternalMaintenanceStore};
use sqlx::PgPool;
use thiserror::Error;

use crate::{
    configs::pipeline::DuckLakeMaintenanceConfig,
    k8s::{DuckLakeMaintenanceResourceConfig, K8sClient, K8sError},
};

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
    pub policy: DuckLakeMaintenanceConfig,
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
    #[error("Kubernetes maintenance materialization failed: {0}")]
    Kubernetes(#[from] K8sError),

    #[error("maintenance backend `{0:?}` is not configured in this deployment")]
    BackendNotConfigured(MaintenanceMaterializerKind),

    #[error("Postgres maintenance materialization failed: {0}")]
    Postgres(#[from] etl::error::EtlError),
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

/// Kubernetes materializer used by Supabase infrastructure.
pub struct KubernetesMaintenanceMaterializer<'a> {
    k8s_client: &'a dyn K8sClient,
}

impl<'a> KubernetesMaintenanceMaterializer<'a> {
    pub fn new(k8s_client: &'a dyn K8sClient) -> Self {
        Self { k8s_client }
    }
}

#[async_trait]
impl MaintenanceMaterializer for KubernetesMaintenanceMaterializer<'_> {
    async fn reconcile_ducklake_maintenance(
        &self,
        input: DuckLakeMaintenanceMaterialization,
    ) -> Result<(), MaintenanceMaterializationError> {
        self.k8s_client
            .create_or_update_ducklake_maintenance(
                &input.identity.resource_prefix,
                DuckLakeMaintenanceResourceConfig {
                    tenant_id: input.identity.tenant_id,
                    pipeline_id: input.identity.pipeline_id,
                    replicator_id: input.identity.replicator_id,
                    image: input.runtime_refs.replicator_image,
                    policy: input.policy,
                },
            )
            .await?;

        Ok(())
    }

    async fn delete_ducklake_maintenance(
        &self,
        identity: MaintenanceIdentity,
    ) -> Result<(), MaintenanceMaterializationError> {
        self.k8s_client.delete_ducklake_maintenance(&identity.resource_prefix).await?;

        Ok(())
    }
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
        store.ensure_schema().await?;

        let policy = ExternalMaintenanceOperationPolicy {
            inline_flush_enabled: true,
            merge_adjacent_files_enabled: true,
            rewrite_data_files_enabled: true,
            expire_snapshots_enabled: false,
            cleanup_old_files_enabled: true,
        };
        store.ensure_pipeline_state(policy).await?;

        Ok(())
    }

    async fn delete_ducklake_maintenance(
        &self,
        identity: MaintenanceIdentity,
    ) -> Result<(), MaintenanceMaterializationError> {
        let store = self.store(identity.pipeline_id);
        store.ensure_schema().await?;
        store.delete_pipeline_state().await?;

        Ok(())
    }
}
