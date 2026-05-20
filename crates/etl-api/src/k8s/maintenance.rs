use async_trait::async_trait;
use etl_maintenance::{
    DuckLakeMaintenanceMaterialization, DuckLakeMaintenancePolicy,
    ExternalMaintenanceOperationPolicy, MaintenanceIdentity, MaintenanceMaterializationError,
    MaintenanceMaterializer,
};

use crate::{
    configs::pipeline::DuckLakeMaintenanceConfig,
    k8s::{DuckLakeMaintenanceResourceConfig, K8sClient},
};

/// Converts API-authored DuckLake maintenance config to backend-neutral policy.
pub fn ducklake_maintenance_policy_from_config(
    config: DuckLakeMaintenanceConfig,
) -> DuckLakeMaintenancePolicy {
    DuckLakeMaintenancePolicy {
        min_interval_seconds: config.min_interval_seconds,
        max_pause_seconds: config.max_pause_seconds,
        min_inlined_bytes: config.min_inlined_bytes,
        max_compacted_files: config.max_compacted_files,
        max_tables_per_run: config.max_tables_per_run,
        target_file_size: config.target_file_size,
        delete_threshold: config.delete_threshold,
        min_active_data_files: config.min_active_data_files,
        cpu_request_millicores: config.cpu_request_millicores,
        memory_request_mib: config.memory_request_mib,
        active_deadline_seconds: config.active_deadline_seconds,
        operation_policy: ExternalMaintenanceOperationPolicy::default(),
    }
}

impl From<DuckLakeMaintenancePolicy> for DuckLakeMaintenanceConfig {
    fn from(policy: DuckLakeMaintenancePolicy) -> Self {
        Self {
            min_interval_seconds: policy.min_interval_seconds,
            max_pause_seconds: policy.max_pause_seconds,
            min_inlined_bytes: policy.min_inlined_bytes,
            max_compacted_files: policy.max_compacted_files,
            max_tables_per_run: policy.max_tables_per_run,
            target_file_size: policy.target_file_size,
            delete_threshold: policy.delete_threshold,
            min_active_data_files: policy.min_active_data_files,
            cpu_request_millicores: policy.cpu_request_millicores,
            memory_request_mib: policy.memory_request_mib,
            active_deadline_seconds: policy.active_deadline_seconds,
        }
    }
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
            .await
            .map_err(MaintenanceMaterializationError::kubernetes)?;

        Ok(())
    }

    async fn delete_ducklake_maintenance(
        &self,
        identity: MaintenanceIdentity,
    ) -> Result<(), MaintenanceMaterializationError> {
        self.k8s_client
            .delete_ducklake_maintenance(&identity.resource_prefix)
            .await
            .map_err(MaintenanceMaterializationError::kubernetes)?;

        Ok(())
    }
}
