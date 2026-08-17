#![allow(dead_code)]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use etl_api::{
    configs::pipeline::ReplicatorResourcesConfig,
    k8s::{
        DuckLakeMaintenanceResourceConfig, K8sClient, K8sError, PipelineRuntimeIdentity, PodStatus,
        ReplicatorConfigMapFile, ReplicatorStatefulSetConfig,
    },
};
use tokio::sync::RwLock;

#[derive(Clone)]
pub(crate) struct MockK8sState {
    pod_status: Arc<RwLock<PodStatus>>,
    create_calls: Arc<AtomicUsize>,
    vpa_delete_calls: Arc<AtomicUsize>,
    ducklake_maintenance_create_calls: Arc<AtomicUsize>,
    last_replicator_image: Arc<RwLock<Option<String>>>,
    last_replicator_resources: Arc<RwLock<Option<ReplicatorResourcesConfig>>>,
}

impl Default for MockK8sState {
    fn default() -> Self {
        Self {
            pod_status: Arc::new(RwLock::new(PodStatus::Started)),
            create_calls: Arc::new(AtomicUsize::new(0)),
            vpa_delete_calls: Arc::new(AtomicUsize::new(0)),
            ducklake_maintenance_create_calls: Arc::new(AtomicUsize::new(0)),
            last_replicator_image: Arc::new(RwLock::new(None)),
            last_replicator_resources: Arc::new(RwLock::new(None)),
        }
    }
}

impl MockK8sState {
    pub(crate) async fn set_pod_status(&self, pod_status: PodStatus) {
        *self.pod_status.write().await = pod_status;
    }

    pub(crate) fn create_calls(&self) -> usize {
        self.create_calls.load(Ordering::Relaxed)
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

    pub(crate) async fn last_replicator_resources(&self) -> Option<ReplicatorResourcesConfig> {
        self.last_replicator_resources.read().await.clone()
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

    async fn set_last_replicator_resources(
        &self,
        replicator_resources: Option<&ReplicatorResourcesConfig>,
    ) {
        *self.state.last_replicator_resources.write().await = replicator_resources.cloned();
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

    async fn delete_postgres_secret(&self, _resource_prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_clickhouse_secret(&self, _resource_prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_bigquery_secret(&self, _resource_prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_iceberg_secret(&self, _resource_prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_ducklake_secret(&self, _resource_prefix: &str) -> Result<(), K8sError> {
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

    async fn delete_snowflake_secret(&self, _resource_prefix: &str) -> Result<(), K8sError> {
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

    async fn delete_replicator_config_map(&self, _resource_prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn create_or_update_replicator_stateful_set(
        &self,
        _resource_prefix: &str,
        _identity: &PipelineRuntimeIdentity,
        config: ReplicatorStatefulSetConfig,
    ) -> Result<(), K8sError> {
        *self.state.last_replicator_image.write().await = Some(config.replicator_image);
        self.set_last_replicator_resources(config.replicator_resources.as_ref()).await;
        self.record_create_call();
        Ok(())
    }

    async fn create_or_update_replicator_vertical_pod_autoscaler(
        &self,
        _resource_prefix: &str,
        _identity: &PipelineRuntimeIdentity,
    ) -> Result<(), K8sError> {
        self.record_create_call();
        Ok(())
    }

    async fn delete_replicator_stateful_set(&self, _resource_prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_replicator_vertical_pod_autoscaler(
        &self,
        _resource_prefix: &str,
    ) -> Result<(), K8sError> {
        self.state.vpa_delete_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn replicator_stateful_set_exists(
        &self,
        _resource_prefix: &str,
    ) -> Result<bool, K8sError> {
        Ok(false)
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

    async fn delete_ducklake_maintenance(&self, _resource_prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn get_replicator_pod_status(
        &self,
        _resource_prefix: &str,
    ) -> Result<PodStatus, K8sError> {
        Ok(*self.state.pod_status.read().await)
    }
}
