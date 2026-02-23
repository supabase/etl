use async_trait::async_trait;
use etl_config::Environment;
use k8s_openapi::api::core::v1::ConfigMap;
use thiserror::Error;

use crate::configs::{destination::StoredDestinationConfig, log::LogLevel};

/// Errors from Kubernetes operations.
///
/// Wraps underlying library errors to preserve context and provide a unified error type
/// for all Kubernetes interactions.
#[derive(Debug, Error)]
pub enum K8sError {
    /// Serialization or deserialization failed when building or parsing Kubernetes resources.
    #[error("An error occurred in serde when dealing with K8s: {0}")]
    Serde(#[from] serde_json::error::Error),
    /// The [`kube`] client returned an error when communicating with the API server.
    #[error("An error occurred with kube when dealing with K8s: {0}")]
    Kube(#[from] kube::Error),
}

/// A file to be stored in a [`ConfigMap`] that is used to configure a replicator.
///
/// Each file becomes a key-value pair in the config map's data section.
#[derive(Debug, Clone)]
pub struct ReplicatorConfigMapFile {
    /// The filename to use as the key in the config map.
    pub filename: String,
    /// The file content to store.
    pub content: String,
}

/// The type of destination storage system for replication.
///
/// Determines which destination-specific resources and configurations are created
/// when deploying a replicator.
#[derive(Debug, Clone, Copy)]
pub enum DestinationType {
    /// Google BigQuery destination.
    BigQuery,
    /// Apache Iceberg destination.
    Iceberg,
}

impl From<&StoredDestinationConfig> for DestinationType {
    /// Extracts the destination type from a stored configuration.
    fn from(value: &StoredDestinationConfig) -> DestinationType {
        match value {
            StoredDestinationConfig::BigQuery { .. } => DestinationType::BigQuery,
            StoredDestinationConfig::Iceberg { .. } => DestinationType::Iceberg,
        }
    }
}

/// A subset of Kubernetes pod phases relevant to the API.
///
/// Maps the standard Kubernetes pod phase strings to a simplified enum.
/// Unrecognized phases are represented as [`PodPhase::Unknown`].
#[derive(Debug)]
pub enum PodPhase {
    /// Pod is waiting to be scheduled or for containers to start.
    Pending,
    /// Pod is bound to a node and at least one container is running.
    Running,
    /// All containers in the pod have terminated successfully.
    Succeeded,
    /// All containers have terminated and at least one failed.
    Failed,
    /// The pod phase could not be determined or is not recognized.
    Unknown,
}

impl From<&str> for PodPhase {
    /// Parses a Kubernetes pod phase string into a [`PodPhase`].
    ///
    /// Returns [`PodPhase::Unknown`] for unrecognized values.
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

/// The derived status of a replicator pod.
///
/// Combines the pod's phase, deletion timestamp, and exit status to determine
/// the operational state from the API's perspective.
pub enum PodStatus {
    /// Pod has successfully stopped and no longer exists.
    Stopped,
    /// Pod is pending or initializing.
    Starting,
    /// Pod is running and ready.
    Started,
    /// Pod is terminating after a deletion request.
    Stopping,
    /// Pod failed to start or exited with an error.
    Failed,
    /// Pod status could not be determined.
    Unknown,
}

/// Operations for managing Kubernetes resources required by replicators.
///
/// Methods use server-side apply patches to provide idempotent create-or-update semantics
/// where possible. All operations target the data-plane namespace unless otherwise specified.
#[async_trait]
pub trait K8sClient: Send + Sync {
    /// Creates or updates the Postgres password secret for a replicator.
    ///
    /// The secret name is derived from `prefix` and stored in the data-plane namespace.
    async fn create_or_update_postgres_secret(
        &self,
        prefix: &str,
        postgres_password: &str,
    ) -> Result<(), K8sError>;

    /// Creates or updates the BigQuery service account secret for a replicator.
    ///
    /// The secret name is derived from `prefix` and stored in the data-plane namespace.
    async fn create_or_update_bigquery_secret(
        &self,
        prefix: &str,
        bq_service_account_key: &str,
    ) -> Result<(), K8sError>;

    /// Creates or updates the Iceberg credentials secret for a replicator.
    ///
    /// The secret contains the catalog token, S3 access key ID, and S3 secret access key.
    /// The secret name is derived from `prefix` and stored in the data-plane namespace.
    async fn create_or_update_iceberg_secret(
        &self,
        prefix: &str,
        catalog_token: &str,
        s3_access_key_id: &str,
        s3_secret_access_key: &str,
    ) -> Result<(), K8sError>;

    /// Deletes the Postgres password secret for a replicator.
    ///
    /// Does nothing if the secret does not exist.
    async fn delete_postgres_secret(&self, prefix: &str) -> Result<(), K8sError>;

    /// Deletes the BigQuery service account secret for a replicator.
    ///
    /// Does nothing if the secret does not exist.
    async fn delete_bigquery_secret(&self, prefix: &str) -> Result<(), K8sError>;

    /// Deletes the Iceberg credentials secret for a replicator.
    ///
    /// Does nothing if the secret does not exist.
    async fn delete_iceberg_secret(&self, prefix: &str) -> Result<(), K8sError>;

    /// Retrieves a [`ConfigMap`] by name from the data-plane namespace.
    async fn get_config_map(&self, config_map_name: &str) -> Result<ConfigMap, K8sError>;

    /// Creates or updates the replicator configuration [`ConfigMap`].
    ///
    /// Accepts a list of files to store in the config map. Each file's filename
    /// becomes a key in the config map's data section with the content as its value.
    /// The config map name is derived from `prefix`.
    async fn create_or_update_replicator_config_map(
        &self,
        prefix: &str,
        files: Vec<ReplicatorConfigMapFile>,
    ) -> Result<(), K8sError>;

    /// Deletes the replicator configuration [`ConfigMap`].
    ///
    /// Does nothing if the config map does not exist.
    async fn delete_replicator_config_map(&self, prefix: &str) -> Result<(), K8sError>;

    /// Creates or updates the replicator [`StatefulSet`].
    ///
    /// The stateful set references secrets and config maps created by other methods.
    /// Changing the configuration may trigger a rolling restart of the pods.
    async fn create_or_update_replicator_stateful_set(
        &self,
        prefix: &str,
        replicator_image: &str,
        environment: Environment,
        destination_type: DestinationType,
        log_level: LogLevel,
    ) -> Result<(), K8sError>;

    /// Deletes the replicator [`StatefulSet`].
    ///
    /// Does nothing if the stateful set does not exist.
    async fn delete_replicator_stateful_set(&self, prefix: &str) -> Result<(), K8sError>;

    /// Retrieves the current status of a replicator pod.
    ///
    /// Returns a [`PodStatus`] derived from the pod's phase, deletion timestamp,
    /// and exit status.
    async fn get_replicator_pod_status(&self, prefix: &str) -> Result<PodStatus, K8sError>;
}
