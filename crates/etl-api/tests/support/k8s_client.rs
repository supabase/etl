#![allow(dead_code)]

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use async_trait::async_trait;
use etl_api::{
    configs::pipeline::PipelineReplicatorResourceOverrideConfig,
    k8s::{
        DuckLakeMaintenanceResourceConfig, K8sClient, K8sError, PipelineRuntimeIdentity, PodStatus,
        ReplicatorConfigMapFile, ReplicatorWorkloadConfig,
    },
};
use tokio::sync::{Notify, RwLock};

/// One-shot barrier for observing and releasing a Kubernetes deletion.
///
/// Dropping the test handle also releases the request after a failed assertion.
#[derive(Clone, Default)]
pub(crate) struct DeletionGate {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl DeletionGate {
    /// Waits until the request reaches deletion while holding its API locks.
    pub(crate) async fn wait_until_entered(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.entered.notified())
            .await
            .unwrap();
    }

    /// Allows the paused request to complete.
    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

impl Drop for DeletionGate {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Clone)]
pub(crate) struct MockK8sState {
    pod_status: Arc<RwLock<PodStatus>>,
    create_calls: Arc<AtomicUsize>,
    stateful_set_apply_calls: Arc<AtomicUsize>,
    vpa_delete_calls: Arc<AtomicUsize>,
    deletion_timeout: Arc<AtomicBool>,
    deletion_gate: Arc<RwLock<Option<DeletionGate>>>,
    restart_completion_calls: Arc<AtomicUsize>,
    restart_pending: Arc<AtomicBool>,
    waited_for_deletion: Arc<AtomicBool>,
    stateful_set_active: Arc<AtomicBool>,
    ducklake_maintenance_create_calls: Arc<AtomicUsize>,
    last_replicator_image: Arc<RwLock<Option<String>>>,
    last_replicator_resource_override:
        Arc<RwLock<Option<PipelineReplicatorResourceOverrideConfig>>>,
}

impl Default for MockK8sState {
    fn default() -> Self {
        Self {
            pod_status: Arc::new(RwLock::new(PodStatus::Started)),
            create_calls: Arc::new(AtomicUsize::new(0)),
            stateful_set_apply_calls: Arc::new(AtomicUsize::new(0)),
            vpa_delete_calls: Arc::new(AtomicUsize::new(0)),
            deletion_timeout: Arc::new(AtomicBool::new(false)),
            deletion_gate: Arc::new(RwLock::new(None)),
            restart_completion_calls: Arc::new(AtomicUsize::new(0)),
            restart_pending: Arc::new(AtomicBool::new(false)),
            waited_for_deletion: Arc::new(AtomicBool::new(false)),
            stateful_set_active: Arc::new(AtomicBool::new(true)),
            ducklake_maintenance_create_calls: Arc::new(AtomicUsize::new(0)),
            last_replicator_image: Arc::new(RwLock::new(None)),
            last_replicator_resource_override: Arc::new(RwLock::new(None)),
        }
    }
}

impl MockK8sState {
    /// Pauses the next workload deletion until the test releases its gate.
    pub(crate) async fn pause_deletion(&self) -> DeletionGate {
        let gate = DeletionGate::default();
        *self.deletion_gate.write().await = Some(gate.clone());
        gate
    }

    /// Controls whether an accepted Pod replacement remains pending.
    pub(crate) fn set_restart_pending(&self, pending: bool) {
        self.restart_pending.store(pending, Ordering::Relaxed);
    }

    /// Counts retries that drive an accepted replacement without changing its
    /// template.
    pub(crate) fn restart_completion_calls(&self) -> usize {
        self.restart_completion_calls.load(Ordering::Relaxed)
    }

    /// Controls whether workload deletion times out.
    pub(crate) fn set_deletion_timeout(&self, timeout: bool) {
        self.deletion_timeout.store(timeout, Ordering::Relaxed);
    }

    /// Reports whether the caller requested the workload termination barrier.
    pub(crate) fn waited_for_deletion(&self) -> bool {
        self.waited_for_deletion.load(Ordering::Relaxed)
    }

    pub(crate) async fn set_pod_status(&self, pod_status: PodStatus) {
        *self.pod_status.write().await = pod_status;
    }

    /// Controls whether the StatefulSet represents active desired state.
    pub(crate) fn set_stateful_set_active(&self, active: bool) {
        self.stateful_set_active.store(active, Ordering::Relaxed);
    }

    pub(crate) fn create_calls(&self) -> usize {
        self.create_calls.load(Ordering::Relaxed)
    }

    /// Counts workload applications independently of their supporting
    /// resources.
    pub(crate) fn stateful_set_apply_calls(&self) -> usize {
        self.stateful_set_apply_calls.load(Ordering::Relaxed)
    }

    pub(crate) fn vpa_delete_calls(&self) -> usize {
        self.vpa_delete_calls.load(Ordering::Relaxed)
    }

    pub(crate) fn ducklake_maintenance_create_calls(&self) -> usize {
        self.ducklake_maintenance_create_calls.load(Ordering::Relaxed)
    }

    pub(crate) async fn last_replicator_image(&self) -> Option<String> {
        self.last_replicator_image.read().await.clone()
    }

    pub(crate) async fn last_replicator_resource_override(
        &self,
    ) -> Option<PipelineReplicatorResourceOverrideConfig> {
        self.last_replicator_resource_override.read().await.clone()
    }
}

pub(crate) struct MockK8sClient {
    state: MockK8sState,
}

impl MockK8sClient {
    pub(crate) fn new(state: MockK8sState) -> Self {
        Self { state }
    }

    fn record_create_call(&self) {
        self.state.create_calls.fetch_add(1, Ordering::Relaxed);
    }

    async fn set_last_replicator_resource_override(
        &self,
        replicator_resource_override: Option<&PipelineReplicatorResourceOverrideConfig>,
    ) {
        *self.state.last_replicator_resource_override.write().await =
            replicator_resource_override.cloned();
    }
}

#[async_trait]
impl K8sClient for MockK8sClient {
    async fn create_or_update_postgres_secret(
        &self,
        _resource_prefix: &str,
        _identity: &PipelineRuntimeIdentity,
        _postgres_password: &str,
    ) -> Result<(), K8sError> {
        self.record_create_call();
        Ok(())
    }

    async fn create_or_update_bigquery_secret(
        &self,
        _resource_prefix: &str,
        _identity: &PipelineRuntimeIdentity,
        _bq_service_account_key: &str,
    ) -> Result<(), K8sError> {
        self.record_create_call();
        Ok(())
    }

    async fn create_or_update_clickhouse_secret(
        &self,
        _resource_prefix: &str,
        _identity: &PipelineRuntimeIdentity,
        _password: Option<&str>,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn create_or_update_iceberg_secret(
        &self,
        _resource_prefix: &str,
        _identity: &PipelineRuntimeIdentity,
        _catalog_token: &str,
        _s3_access_key_id: &str,
        _s3_secret_access_key: &str,
    ) -> Result<(), K8sError> {
        self.record_create_call();
        Ok(())
    }

    async fn create_or_update_ducklake_secret(
        &self,
        _resource_prefix: &str,
        _identity: &PipelineRuntimeIdentity,
        _catalog_url: &str,
        _s3_access_key_id: &str,
        _s3_secret_access_key: &str,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_postgres_secret(
        &self,
        _resource_prefix: &str,
        _wait: bool,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_clickhouse_secret(
        &self,
        _resource_prefix: &str,
        _wait: bool,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_bigquery_secret(
        &self,
        _resource_prefix: &str,
        _wait: bool,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_iceberg_secret(
        &self,
        _resource_prefix: &str,
        _wait: bool,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_ducklake_secret(
        &self,
        _resource_prefix: &str,
        _wait: bool,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn create_or_update_snowflake_secret(
        &self,
        _resource_prefix: &str,
        _identity: &PipelineRuntimeIdentity,
        _private_key: &str,
        _private_key_passphrase: Option<&str>,
    ) -> Result<(), K8sError> {
        self.record_create_call();
        Ok(())
    }

    async fn delete_snowflake_secret(
        &self,
        _resource_prefix: &str,
        _wait: bool,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn create_or_update_replicator_config_map(
        &self,
        _resource_prefix: &str,
        _identity: &PipelineRuntimeIdentity,
        _files: Vec<ReplicatorConfigMapFile>,
    ) -> Result<(), K8sError> {
        self.record_create_call();
        Ok(())
    }

    async fn delete_replicator_config_map(
        &self,
        _resource_prefix: &str,
        _wait: bool,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn create_or_update_replicator_stateful_set(
        &self,
        _resource_prefix: &str,
        _identity: &PipelineRuntimeIdentity,
        workload_config: &ReplicatorWorkloadConfig,
    ) -> Result<(), K8sError> {
        self.state.stateful_set_apply_calls.fetch_add(1, Ordering::Relaxed);
        *self.state.last_replicator_image.write().await =
            Some(workload_config.replicator_image.clone());
        self.set_last_replicator_resource_override(
            workload_config.replicator_resource_override.as_ref(),
        )
        .await;
        self.record_create_call();
        Ok(())
    }

    async fn create_or_update_replicator_vertical_pod_autoscaler(
        &self,
        _resource_prefix: &str,
        _identity: &PipelineRuntimeIdentity,
        _workload_config: &ReplicatorWorkloadConfig,
    ) -> Result<(), K8sError> {
        self.record_create_call();
        Ok(())
    }

    async fn complete_pending_replicator_restart(
        &self,
        _resource_prefix: &str,
    ) -> Result<bool, K8sError> {
        let pending = self.state.restart_pending.load(Ordering::Relaxed);
        if pending {
            self.state.restart_completion_calls.fetch_add(1, Ordering::Relaxed);
        }
        Ok(pending)
    }

    async fn delete_replicator_stateful_set(
        &self,
        resource_prefix: &str,
        wait: bool,
    ) -> Result<(), K8sError> {
        self.state.waited_for_deletion.store(wait, Ordering::Relaxed);
        let gate = self.state.deletion_gate.write().await.take();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        if wait && self.state.deletion_timeout.load(Ordering::Relaxed) {
            return Err(K8sError::ResourceDeletionTimeout {
                kind: "StatefulSet",
                name: resource_prefix.to_owned(),
                timeout_seconds: 30,
            });
        }
        Ok(())
    }

    async fn delete_replicator_vertical_pod_autoscaler(
        &self,
        _resource_prefix: &str,
        _wait: bool,
    ) -> Result<(), K8sError> {
        self.state.vpa_delete_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn replicator_stateful_set_is_active(
        &self,
        _resource_prefix: &str,
    ) -> Result<bool, K8sError> {
        Ok(self.state.stateful_set_active.load(Ordering::Relaxed))
    }

    async fn create_or_update_ducklake_maintenance(
        &self,
        _resource_prefix: &str,
        _identity: &PipelineRuntimeIdentity,
        _config: DuckLakeMaintenanceResourceConfig,
    ) -> Result<(), K8sError> {
        self.state.ducklake_maintenance_create_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn delete_ducklake_maintenance(
        &self,
        _resource_prefix: &str,
        _wait: bool,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn get_replicator_pod_status(
        &self,
        _resource_prefix: &str,
    ) -> Result<PodStatus, K8sError> {
        Ok(*self.state.pod_status.read().await)
    }
}
