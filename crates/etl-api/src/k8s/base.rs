use async_trait::async_trait;
use etl_maintenance::DuckLakeMaintenancePolicy;
use thiserror::Error;

use crate::configs::{
    destination::StoredDestinationConfig,
    log::LogLevel,
    pipeline::{DuckLakeMaintenanceConfig, PipelineReplicatorResourceOverrideConfig},
};

/// Errors from Kubernetes operations.
///
/// Wraps underlying library errors to preserve context and provide a unified
/// error type for all Kubernetes interactions.
#[derive(Debug, Error)]
pub enum K8sError {
    /// Runtime environment configuration could not be loaded.
    #[error("Failed to load Kubernetes runtime environment")]
    Config(#[source] std::io::Error),
    /// Serialization or deserialization failed when building or parsing
    /// Kubernetes resources.
    #[error("An error occurred in serde when dealing with K8s: {0}")]
    Serde(#[from] serde_json::error::Error),
    /// The [`kube`] client returned an error when communicating with the API
    /// server.
    #[error("An error occurred with kube when dealing with K8s: {0}")]
    Kube(#[from] kube::Error),
    /// A Kubernetes resource remained present after deletion was requested.
    #[error(
        "Timed out waiting for Kubernetes {kind} resource '{name}' to be deleted after \
         {timeout_seconds} seconds"
    )]
    ResourceDeletionTimeout {
        /// Kubernetes resource kind.
        kind: &'static str,
        /// Kubernetes resource name.
        name: String,
        /// Deletion timeout in seconds.
        timeout_seconds: u64,
    },
}

/// A file to be stored in a [`ConfigMap`] that is used to configure a
/// replicator.
///
/// Each file becomes a key-value pair in the config map's data section.
#[derive(Debug, Clone)]
pub struct ReplicatorConfigMapFile {
    /// The filename to use as the key in the config map.
    pub filename: String,
    /// The file content to store.
    pub content: String,
}

/// Product identity shared by all Kubernetes resources in a pipeline runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineRuntimeIdentity {
    /// Tenant/project identifier.
    pub tenant_id: String,
    /// Pipeline identifier.
    pub pipeline_id: i64,
    /// Concrete replicator identifier for this pipeline runtime.
    pub replicator_id: i64,
}

/// DuckLake maintenance CR materialization input.
#[derive(Debug, Clone)]
pub struct DuckLakeMaintenanceResourceConfig {
    /// Image containing the maintenance binary.
    pub image: String,
    /// User-authored maintenance policy.
    pub policy: DuckLakeMaintenancePolicy,
}

/// Input shared by the replicator StatefulSet and VPA materializers.
#[derive(Debug, Clone)]
pub struct ReplicatorWorkloadConfig {
    /// Image for the replicator container.
    pub replicator_image: String,
    /// Optional pipeline-level replicator resource override.
    pub replicator_resource_override: Option<PipelineReplicatorResourceOverrideConfig>,
    /// Destination type used to select destination-specific env/secrets.
    pub destination_type: DestinationType,
    /// DuckLake maintenance policy.
    pub ducklake_maintenance: Option<DuckLakeMaintenanceConfig>,
    /// Replicator log level.
    pub log_level: LogLevel,
}

/// The type of destination storage system for replication.
///
/// Determines which destination-specific resources and configurations are
/// created when deploying a replicator.
#[derive(Debug, Clone, Copy)]
pub enum DestinationType {
    /// Google BigQuery destination.
    BigQuery,
    /// Apache Iceberg destination.
    Iceberg,
    /// ClickHouse destination.
    ClickHouse {
        /// Whether the StatefulSet must reference the ClickHouse password
        /// secret.
        password_secret_required: bool,
    },
    /// DuckLake destination.
    Ducklake,
    /// Snowflake destination.
    Snowflake {
        /// Whether the StatefulSet must reference the Snowflake passphrase
        /// secret entry.
        passphrase_secret_required: bool,
    },
}

impl From<&StoredDestinationConfig> for DestinationType {
    /// Extracts the destination type from a stored configuration.
    fn from(value: &StoredDestinationConfig) -> DestinationType {
        match value {
            StoredDestinationConfig::BigQuery { .. } => DestinationType::BigQuery,
            StoredDestinationConfig::Iceberg { .. } => DestinationType::Iceberg,
            StoredDestinationConfig::ClickHouse { password, .. } => {
                DestinationType::ClickHouse { password_secret_required: password.is_some() }
            }
            StoredDestinationConfig::Ducklake { .. } => DestinationType::Ducklake,
            StoredDestinationConfig::Snowflake { private_key_passphrase, .. } => {
                DestinationType::Snowflake {
                    passphrase_secret_required: private_key_passphrase.is_some(),
                }
            }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
/// Methods use server-side apply patches to provide idempotent create-or-update
/// semantics where possible. All operations target the data-plane namespace
/// unless otherwise specified.
#[async_trait]
pub trait K8sClient: Send + Sync {
    /// Creates or updates the Postgres password secret for a replicator.
    ///
    /// The secret name is derived from `resource_prefix` and stored in the
    /// data-plane namespace.
    async fn create_or_update_postgres_secret(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        postgres_password: &str,
    ) -> Result<(), K8sError>;

    /// Creates or updates the BigQuery service account secret for a replicator.
    ///
    /// The secret name is derived from `resource_prefix` and stored in the
    /// data-plane namespace.
    async fn create_or_update_bigquery_secret(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        bq_service_account_key: &str,
    ) -> Result<(), K8sError>;

    /// Creates or updates the ClickHouse password secret for a replicator.
    async fn create_or_update_clickhouse_secret(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        password: Option<&str>,
    ) -> Result<(), K8sError>;

    /// Creates or updates the Iceberg credentials secret for a replicator.
    ///
    /// The secret contains the catalog token, S3 access key ID, and S3 secret
    /// access key. The secret name is derived from `resource_prefix` and stored
    /// in the data-plane namespace.
    async fn create_or_update_iceberg_secret(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        catalog_token: &str,
        s3_access_key_id: &str,
        s3_secret_access_key: &str,
    ) -> Result<(), K8sError>;

    /// Creates or updates the DuckLake credentials secret for a replicator.
    ///
    /// The secret contains the catalog URL and S3 credentials.
    async fn create_or_update_ducklake_secret(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        catalog_url: &str,
        s3_access_key_id: &str,
        s3_secret_access_key: &str,
    ) -> Result<(), K8sError>;

    /// Deletes the Postgres password secret for a replicator.
    ///
    /// Does nothing if the secret does not exist.
    async fn delete_postgres_secret(&self, resource_prefix: &str) -> Result<(), K8sError>;

    /// Deletes the ClickHouse credentials for a replicator.
    ///
    /// Does nothing if the secret does not exist.
    async fn delete_clickhouse_secret(&self, resource_prefix: &str) -> Result<(), K8sError>;

    /// Deletes the BigQuery service account secret for a replicator.
    ///
    /// Does nothing if the secret does not exist.
    async fn delete_bigquery_secret(&self, resource_prefix: &str) -> Result<(), K8sError>;

    /// Deletes the Iceberg credentials secret for a replicator.
    ///
    /// Does nothing if the secret does not exist.
    async fn delete_iceberg_secret(&self, resource_prefix: &str) -> Result<(), K8sError>;

    /// Deletes the DuckLake credentials secret for a replicator.
    ///
    /// Does nothing if the secret does not exist.
    async fn delete_ducklake_secret(&self, resource_prefix: &str) -> Result<(), K8sError>;

    /// Creates or updates the Snowflake credentials secret for a replicator.
    ///
    /// The secret contains the RSA private key and optionally the private key
    /// passphrase.
    async fn create_or_update_snowflake_secret(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        private_key: &str,
        private_key_passphrase: Option<&str>,
    ) -> Result<(), K8sError>;

    /// Deletes the Snowflake credentials secret for a replicator.
    ///
    /// Does nothing if the secret does not exist.
    async fn delete_snowflake_secret(&self, resource_prefix: &str) -> Result<(), K8sError>;

    /// Creates or updates the replicator configuration [`ConfigMap`].
    ///
    /// Accepts a list of files to store in the config map. Each file's filename
    /// becomes a key in the config map's data section with the content as its
    /// value. The config map name is derived from `resource_prefix`.
    async fn create_or_update_replicator_config_map(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        files: Vec<ReplicatorConfigMapFile>,
    ) -> Result<(), K8sError>;

    /// Deletes the replicator configuration [`ConfigMap`].
    ///
    /// Does nothing if the config map does not exist.
    async fn delete_replicator_config_map(&self, resource_prefix: &str) -> Result<(), K8sError>;

    /// Creates or updates the replicator `StatefulSet`.
    ///
    /// The stateful set references secrets and config maps created by other
    /// methods. Applying this resource intentionally changes the pod template
    /// restart annotation so the StatefulSet recreates its pods. This ensures
    /// the replicator process observes newly materialized mounted config and
    /// secret-backed environment values.
    async fn create_or_update_replicator_stateful_set(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        workload_config: &ReplicatorWorkloadConfig,
    ) -> Result<(), K8sError>;

    /// Creates or updates the Vertical Pod Autoscaler for the replicator
    /// `StatefulSet`.
    ///
    /// This is called only when neither pipeline resource request is
    /// overridden. Configured autoscaling bounds are independent of the
    /// StatefulSet startup allocation. When autoscaling is omitted, the startup
    /// allocation is used as both VPA bounds.
    async fn create_or_update_replicator_vertical_pod_autoscaler(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        workload_config: &ReplicatorWorkloadConfig,
    ) -> Result<(), K8sError>;

    /// Deletes the replicator `StatefulSet`.
    ///
    /// Does nothing if the stateful set does not exist.
    async fn delete_replicator_stateful_set(&self, resource_prefix: &str) -> Result<(), K8sError>;

    /// Deletes the replicator Vertical Pod Autoscaler.
    ///
    /// Does nothing if the autoscaler does not exist.
    async fn delete_replicator_vertical_pod_autoscaler(
        &self,
        resource_prefix: &str,
    ) -> Result<(), K8sError>;

    /// Returns whether the replicator `StatefulSet` exists.
    async fn replicator_stateful_set_exists(&self, resource_prefix: &str)
    -> Result<bool, K8sError>;

    /// Creates or updates the DuckLake maintenance CR.
    async fn create_or_update_ducklake_maintenance(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        config: DuckLakeMaintenanceResourceConfig,
    ) -> Result<(), K8sError>;

    /// Deletes the DuckLake maintenance CR and waits until it is absent.
    async fn delete_ducklake_maintenance(&self, resource_prefix: &str) -> Result<(), K8sError>;

    /// Retrieves the current status of a replicator pod.
    ///
    /// Returns a [`PodStatus`] derived from the pod's phase, deletion
    /// timestamp, and exit status.
    async fn get_replicator_pod_status(&self, resource_prefix: &str)
    -> Result<PodStatus, K8sError>;
}
