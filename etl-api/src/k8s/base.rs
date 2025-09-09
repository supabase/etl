//! Abstractions shared by Kubernetes clients.
//!
//! This module defines the error type, a minimal pod phase enum, and the
//! [`K8sClient`] trait used by the API to interact with Kubernetes.

use async_trait::async_trait;
use k8s_openapi::api::core::v1::ConfigMap;
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error)]
/// Errors emitted by the Kubernetes integration.
///
/// Variants wrap lower-level libraries where appropriate to preserve context.
pub enum K8sError {
    #[error("An error occurred in serde: {0}")]
    /// A serialization or deserialization error while building or parsing
    /// Kubernetes resources.
    Serde(#[from] serde_json::error::Error),
    #[error("An error occurred with kube: {0}")]
    /// An error returned by the [`kube`] client when talking to the API
    /// server.
    Kube(#[from] kube::Error),
    #[error("An error occurred while configuring the replicator")]
    /// The environment-dependent replicator configuration could not be
    /// determined.
    ReplicatorConfiguration,
}

/// A simplified view of a pod phase.
///
/// This mirrors the string phases reported by Kubernetes but only tracks the
/// states needed by the API. Unknown values map to [`PodPhase::Unknown`].
pub enum PodPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

impl From<&str> for PodPhase {
    /// Converts a Kubernetes pod phase string into a [`PodPhase`].
    ///
    /// Unrecognized values result in [`PodPhase::Unknown`].
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
/// Client interface describing the Kubernetes operations used by the API.
///
/// Implementations are expected to be idempotent where possible by issuing
/// server-side apply patches for create-or-update behaviors.
pub trait K8sClient: Send + Sync {
    /// Creates or updates the Postgres password secret for a replicator.
    ///
    /// The secret name is derived from `prefix` and is stored in the
    /// data-plane namespace.
    ///
    /// # Errors
    /// Returns [`K8sError`] if the request to the API server fails or the
    /// resource cannot be serialized.
    async fn create_or_update_postgres_secret(
        &self,
        prefix: &str,
        postgres_password: &str,
    ) -> Result<(), K8sError>;

    /// Creates or updates the BigQuery service account secret for a
    /// replicator.
    ///
    /// The secret name is derived from `prefix` and is stored in the
    /// data-plane namespace.
    ///
    /// # Errors
    /// Returns [`K8sError`] if the request to the API server fails or the
    /// resource cannot be serialized.
    async fn create_or_update_bq_secret(
        &self,
        prefix: &str,
        bq_service_account_key: &str,
    ) -> Result<(), K8sError>;

    /// Deletes the Postgres password secret for a replicator if it exists.
    ///
    /// # Errors
    /// Returns [`K8sError`] if the delete request fails with an error other
    /// than a not-found response.
    async fn delete_postgres_secret(&self, prefix: &str) -> Result<(), K8sError>;

    /// Deletes the BigQuery service account secret for a replicator if it
    /// exists.
    ///
    /// # Errors
    /// Returns [`K8sError`] if the delete request fails with an error other
    /// than a not-found response.
    async fn delete_bq_secret(&self, prefix: &str) -> Result<(), K8sError>;

    /// Retrieves a named [`ConfigMap`].
    ///
    /// # Errors
    /// Returns [`K8sError`] if the resource cannot be fetched.
    async fn get_config_map(&self, config_map_name: &str) -> Result<ConfigMap, K8sError>;

    /// Creates or updates the replicator configuration [`ConfigMap`].
    ///
    /// The config map stores two YAML documents: a base and a production
    /// override.
    ///
    /// # Errors
    /// Returns [`K8sError`] if the request to the API server fails or the
    /// resource cannot be serialized.
    async fn create_or_update_config_map(
        &self,
        prefix: &str,
        base_config: &str,
        prod_config: &str,
    ) -> Result<(), K8sError>;

    /// Deletes the replicator configuration [`ConfigMap`] if it exists.
    ///
    /// # Errors
    /// Returns [`K8sError`] if the delete request fails with an error other
    /// than a not-found response.
    async fn delete_config_map(&self, prefix: &str) -> Result<(), K8sError>;

    /// Creates or updates the replicator [`StatefulSet`].
    ///
    /// The set references previously created secrets and config maps. Optional
    /// `template_annotations` may be used to trigger a rolling restart.
    ///
    /// # Errors
    /// Returns [`K8sError`] if the request to the API server fails or the
    /// resource cannot be serialized.
    async fn create_or_update_stateful_set(
        &self,
        prefix: &str,
        replicator_image: &str,
        template_annotations: Option<BTreeMap<String, String>>,
    ) -> Result<(), K8sError>;

    /// Deletes the replicator [`StatefulSet`] if it exists.
    ///
    /// # Errors
    /// Returns [`K8sError`] if the delete request fails with an error other
    /// than a not-found response.
    async fn delete_stateful_set(&self, prefix: &str) -> Result<(), K8sError>;

    /// Returns the phase of the replicator pod.
    ///
    /// # Errors
    /// Returns [`K8sError`] if the pod cannot be queried and the error is not
    /// a handled not-found response.
    async fn get_pod_phase(&self, prefix: &str) -> Result<PodPhase, K8sError>;

    /// Reports whether the replicator container terminated with a non-zero exit
    /// code.
    ///
    /// # Errors
    /// Returns [`K8sError`] if the pod cannot be queried and the error is not
    /// a handled not-found response.
    async fn has_replicator_container_error(&self, prefix: &str) -> Result<bool, K8sError>;

    /// Deletes the replicator pod if it exists.
    ///
    /// # Errors
    /// Returns [`K8sError`] if the delete request fails with an error other
    /// than a not-found response.
    async fn delete_pod(&self, prefix: &str) -> Result<(), K8sError>;
}
