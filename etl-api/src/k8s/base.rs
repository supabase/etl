use std::collections::BTreeMap;
use async_trait::async_trait;
use k8s_openapi::api::core::v1::ConfigMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum K8sError {
    #[error("An error occurred in serde: {0}")]
    Serde(#[from] serde_json::error::Error),
    #[error("An error occurred with kube: {0}")]
    Kube(#[from] kube::Error),
    #[error("An error occurred while configuring the replicator")]
    ReplicatorConfiguration,
}

pub enum PodPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

impl From<&str> for PodPhase {
    fn from(value: &str) -> Self {
        match value {
            "Pending" => PodPhase::Pending,
            "Running" => PodPhase::Running,
            "Succeeded" => PodPhase::Succeeded,
            "Failed" => PodPhase::Failed,
            _ => PodPhase::Unknown,
        }
    }
}

#[async_trait]
pub trait K8sClient: Send + Sync {
    async fn create_or_update_postgres_secret(
        &self,
        prefix: &str,
        postgres_password: &str,
    ) -> Result<(), K8sError>;

    async fn create_or_update_bq_secret(
        &self,
        prefix: &str,
        bq_service_account_key: &str,
    ) -> Result<(), K8sError>;

    async fn delete_postgres_secret(&self, prefix: &str) -> Result<(), K8sError>;

    async fn delete_bq_secret(&self, prefix: &str) -> Result<(), K8sError>;

    async fn get_config_map(&self, config_map_name: &str) -> Result<ConfigMap, K8sError>;

    async fn create_or_update_config_map(
        &self,
        prefix: &str,
        base_config: &str,
        prod_config: &str,
    ) -> Result<(), K8sError>;

    async fn delete_config_map(&self, prefix: &str) -> Result<(), K8sError>;

    async fn create_or_update_stateful_set(
        &self,
        prefix: &str,
        replicator_image: &str,
        template_annotations: Option<BTreeMap<String, String>>,
    ) -> Result<(), K8sError>;

    async fn delete_stateful_set(&self, prefix: &str) -> Result<(), K8sError>;

    async fn get_pod_phase(&self, prefix: &str) -> Result<PodPhase, K8sError>;

    async fn has_replicator_container_error(&self, prefix: &str) -> Result<bool, K8sError>;

    async fn delete_pod(&self, prefix: &str) -> Result<(), K8sError>;
}