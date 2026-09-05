use std::{fmt, path::PathBuf};

use base64::{Engine, prelude::BASE64_STANDARD};
use etl_config::{
    Config,
    shared::{
        DuckLakeCopyBufferConfig, PgConnectionConfig, SentryConfig, TlsConfig, Validate,
        ValidationError,
    },
};
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use thiserror::Error;

/// Required length in bytes for a valid API key.
const API_KEY_LENGTH_IN_BYTES: usize = 32;

/// Complete configuration for the ETL API service.
///
/// Contains all settings required to run the API including database connection,
/// server settings, encryption, authentication, and optional monitoring.
#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    /// Database connection configuration for the API database.
    pub database: PgConnectionConfig,
    /// Application server settings.
    pub application: ApplicationSettings,
    /// Kubernetes-specific API configuration.
    pub k8s: K8sConfig,
    /// Replicator runtime defaults applied when pipelines omit a setting.
    #[serde(default)]
    pub replicator: ApiReplicatorConfig,
    /// Source database configuration and validation settings.
    pub source: SourceConfig,
    /// Encryption key configurations.
    pub encryption_keys: Vec<EncryptionKeyConfig>,
    /// List of base64-encoded API keys.
    ///
    /// All keys in this list are considered valid for authentication.
    pub api_keys: Vec<String>,
    /// Tenant IDs used by staging destination simulators.
    ///
    /// Pipelines belonging to these tenants may select registered non-default
    /// replicator images so each simulator can test an experimental build
    /// without changing the default image for other tenants.
    ///
    /// Production tenant IDs must never be included. An empty list preserves
    /// the default-only image policy for every tenant.
    #[serde(default)]
    pub simulator_tenant_ids: Vec<String>,
    /// Optional Sentry configuration for error tracking.
    pub sentry: Option<SentryConfig>,
    /// Optional Supabase API URL for error notifications.
    ///
    /// When provided, this URL is passed to replicators to enable
    /// error notifications to the Supabase API. The API key will be
    /// injected as a Kubernetes secret named `supabase_api_key`.
    pub supabase_api_url: Option<String>,
    /// Optional ConfigCat SDK key for feature flag integration.
    ///
    /// If provided, enables ConfigCat feature flag evaluation.
    /// If `None`, the API operates without feature flag support.
    pub configcat_sdk_key: Option<String>,
}

/// Errors produced while validating ETL API service configuration.
#[derive(Debug, Error)]
pub enum ApiConfigValidationError {
    /// An existing API-specific validation check failed.
    #[error("{0}")]
    InvalidValue(String),
    /// Shared replicator configuration validation failed.
    #[error(transparent)]
    Replicator(#[from] ValidationError),
}

impl From<String> for ApiConfigValidationError {
    fn from(error: String) -> Self {
        Self::InvalidValue(error)
    }
}

/// Defaults applied to generated replicator configurations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ApiReplicatorConfig {
    /// Defaults grouped by destination kind.
    #[serde(default)]
    pub destination_defaults: DestinationDefaultsConfig,
}

/// Replicator defaults grouped by destination kind.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct DestinationDefaultsConfig {
    /// Defaults for DuckLake destinations.
    #[serde(default)]
    pub ducklake: DuckLakeDestinationDefaultsConfig,
}

/// Replicator defaults for DuckLake destinations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct DuckLakeDestinationDefaultsConfig {
    /// Initial-copy buffering used when a pipeline does not configure it.
    #[serde(default)]
    pub copy_buffer: DuckLakeCopyBufferConfig,
}

/// Kubernetes-specific API configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct K8sConfig {
    /// Namespace where all per-replicator Kubernetes resources are created.
    #[serde(default = "default_replicator_namespace")]
    pub replicator_namespace: String,
    /// ServiceAccount assigned to every replicator pod.
    #[serde(default = "default_replicator_service_account_name")]
    pub replicator_service_account_name: String,
    /// Node selector applied to every replicator pod.
    #[serde(default)]
    pub replicator_node_selectors: Vec<NodeSelectorConfig>,
    /// Tolerations applied to every replicator pod.
    #[serde(default)]
    pub replicator_tolerations: Vec<TolerationConfig>,
    /// API-wide startup request defaults for replicator workloads.
    ///
    /// This key remains `replicator_resources` in API configuration files. It
    /// provides the mandatory baseline CPU and memory requests written to each
    /// replicator pod template unless a pipeline-level override supplies one
    /// of those request values.
    pub replicator_resources: ReplicatorResourceDefaultsConfig,
    /// Optional API-wide VPA interval for replicator CPU and memory.
    ///
    /// A pipeline request override fixes the corresponding resource's VPA
    /// bounds to that request. For resources without an override, omission
    /// fixes the bounds to the resolved startup request, while a configured
    /// interval defines the allowed recommendation range independently of the
    /// startup request.
    #[serde(default)]
    pub replicator_autoscaling: Option<ReplicatorResourceAutoscalingConfig>,
    /// Vector image used by the logging sidecar.
    #[serde(default = "default_vector_image")]
    pub vector_image: String,
    /// API-wide request defaults for the Vector sidecar.
    pub vector_resources: VectorResourceDefaultsConfig,
}

/// API-wide VPA recommendation interval for replicator resources.
///
/// These bounds configure generated VPAs and are independent of the startup
/// requests in [`ReplicatorResourceDefaultsConfig`]. The API-wide startup
/// requests are expected to lie within this interval, but the API does not
/// validate or clamp that relationship. Once VPA actuation begins, an
/// out-of-range request may be moved inside these bounds. A pipeline request
/// override replaces the corresponding interval with a fixed bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct ReplicatorResourceAutoscalingConfig {
    /// Update mode assigned when a per-pipeline VPA is first created.
    #[serde(default)]
    pub initial_update_mode: ReplicatorResourceAutoscalingUpdateMode,
    /// Minimum replicator memory allocation, in Mi.
    pub min_memory_mib: i32,
    /// Maximum replicator memory allocation, in Mi.
    pub max_memory_mib: i32,
    /// Minimum replicator CPU allocation, in millicores.
    pub min_cpu_millicores: i32,
    /// Maximum replicator CPU allocation, in millicores.
    pub max_cpu_millicores: i32,
}

/// Initial Kubernetes VPA update mode for a replicator workload.
///
/// The API creates a VPA for every replicator. [`Self::Off`] keeps that VPA in
/// recommendation-only mode; every other mode allows it to apply resource
/// recommendations. Reconciliation preserves the update mode already present
/// on a live VPA, so this setting only chooses the mode at creation time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicatorResourceAutoscalingUpdateMode {
    /// Publish recommendations without changing Pod resources.
    #[default]
    Off,
    /// Apply recommendations only when Pods are created.
    Initial,
    /// Apply recommendations to new Pods and recreate running Pods when needed.
    Recreate,
    /// Prefer in-place updates and fall back to recreating Pods when required.
    InPlaceOrRecreate,
    /// Update running Pods only in place and never evict them.
    InPlace,
}

impl ReplicatorResourceAutoscalingUpdateMode {
    /// Returns the value expected by the Kubernetes VPA API.
    pub const fn as_k8s_value(self) -> &'static str {
        match self {
            Self::Off => "Off",
            Self::Initial => "Initial",
            Self::Recreate => "Recreate",
            Self::InPlaceOrRecreate => "InPlaceOrRecreate",
            Self::InPlace => "InPlace",
        }
    }
}

fn default_replicator_namespace() -> String {
    "etl-data-plane".to_owned()
}

fn default_replicator_service_account_name() -> String {
    "etl-replicator".to_owned()
}

fn default_vector_image() -> String {
    "timberio/vector:0.55.0-distroless-libc".to_owned()
}

/// Simplified Kubernetes node selector configuration.
///
/// The ETL API passes these strings through without validation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct NodeSelectorConfig {
    pub key: String,
    pub value: String,
}

/// Simplified Kubernetes toleration configuration.
///
/// The ETL API passes these strings through without validation and emits an
/// `Equal` toleration operator.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TolerationConfig {
    pub key: String,
    pub value: String,
    pub effect: String,
}

/// API-wide startup request defaults for replicator workloads.
///
/// The mandatory CPU and memory values apply to every destination.
/// Pipeline-level overrides are applied later. A configured autoscaling
/// interval supplies VPA bounds for resources without an override; otherwise
/// these defaults are used as fixed bounds.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ReplicatorResourceDefaultsConfig {
    /// Replicator startup memory request, in Mi.
    pub memory_request_mib: i32,
    /// Replicator startup CPU request, in millicores.
    pub cpu_request_millicores: i32,
}

/// API-wide request defaults for Vector sidecars.
///
/// These values are part of the ETL API service configuration and provide the
/// baseline Kubernetes requests for Vector containers. Resource limits match
/// these requests.
#[derive(Debug, Clone, Deserialize)]
pub struct VectorResourceDefaultsConfig {
    /// Vector memory request, in Mi.
    pub memory_request_mib: i32,
    /// Vector CPU request, in millicores.
    pub cpu_request_millicores: i32,
}

impl ApiConfig {
    /// Validates API service configuration.
    pub fn validate(&self) -> Result<(), ApiConfigValidationError> {
        self.k8s.replicator_resources.validate()?;
        if let Some(replicator_autoscaling) = &self.k8s.replicator_autoscaling {
            replicator_autoscaling.validate()?;
        }
        self.k8s.vector_resources.validate()?;
        self.replicator.destination_defaults.ducklake.copy_buffer.validate()?;

        Ok(())
    }

    /// Returns whether `tenant_id` identifies an allowlisted staging simulator.
    pub(crate) fn is_simulator_tenant(&self, tenant_id: &str) -> bool {
        self.simulator_tenant_ids.iter().any(|id| id == tenant_id)
    }
}

impl ReplicatorResourceAutoscalingConfig {
    /// Validates that autoscaling bounds are positive and ordered.
    pub fn validate(&self) -> Result<(), String> {
        validate_positive_request("K8s autoscaling minimum memory", self.min_memory_mib)?;
        validate_positive_request("K8s autoscaling maximum memory", self.max_memory_mib)?;
        validate_positive_request("K8s autoscaling minimum cpu", self.min_cpu_millicores)?;
        validate_positive_request("K8s autoscaling maximum cpu", self.max_cpu_millicores)?;

        if self.min_memory_mib > self.max_memory_mib {
            return Err("K8s autoscaling minimum memory must not exceed maximum memory".to_owned());
        }
        if self.min_cpu_millicores > self.max_cpu_millicores {
            return Err("K8s autoscaling minimum cpu must not exceed maximum cpu".to_owned());
        }

        Ok(())
    }
}

impl ReplicatorResourceDefaultsConfig {
    /// Validates that configured request values are positive.
    pub fn validate(&self) -> Result<(), String> {
        validate_positive_request("K8s replicator memory request", self.memory_request_mib)?;
        validate_positive_request("K8s replicator cpu request", self.cpu_request_millicores)?;

        Ok(())
    }
}

impl VectorResourceDefaultsConfig {
    /// Validates that configured request values are positive.
    pub fn validate(&self) -> Result<(), String> {
        validate_positive_request("K8s Vector memory request", self.memory_request_mib)?;
        validate_positive_request("K8s Vector cpu request", self.cpu_request_millicores)?;

        Ok(())
    }
}

fn validate_positive_request(name: &str, value: i32) -> Result<(), String> {
    if value <= 0 {
        return Err(format!("{name} must be greater than 0"));
    }

    Ok(())
}

/// Configuration for source database connections and behavior.
#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    /// TLS configuration for source database connections, mirroring
    /// `database.tls`'s shape so both connections are configured the same
    /// way.
    ///
    /// When `enabled` is `true`, the API uses `trusted_root_certs` to
    /// establish TLS connections to source databases. This applies both to
    /// direct API connections (e.g., listing tables, managing publications)
    /// and to replicator pods deployed in Kubernetes, which receive the same
    /// resolved value embedded in their generated configuration.
    pub tls: TlsConfig,
    /// Optional trusted username for source profile validation.
    ///
    /// When provided, ETL validates that source connections use this role and
    /// that the role matches the expected ETL profile and memberships.
    ///
    /// If `None`, trusted source profile validation is skipped.
    pub trusted_username: Option<String>,
}

impl Config for ApiConfig {
    const LIST_PARSE_KEYS: &'static [&'static str] = &["api_keys", "simulator_tenant_ids"];
}

/// HTTP server configuration settings.
#[derive(Debug, Clone, Deserialize)]
pub struct ApplicationSettings {
    /// Host address the API listens on.
    pub host: String,
    /// Port number the public API listens on.
    pub port: u16,
    /// Port number the cluster-internal API listens on.
    pub internal_port: u16,
    /// Optional TLS certificate and private key for the cluster-internal
    /// listener.
    #[serde(default)]
    pub internal_tls: Option<InternalTlsSettings>,
}

/// TLS files used by the cluster-internal API listener.
#[derive(Debug, Clone, Deserialize)]
pub struct InternalTlsSettings {
    /// PEM-encoded server certificate chain.
    pub cert_path: PathBuf,
    /// PEM-encoded server private key.
    pub key_path: PathBuf,
}

impl fmt::Display for ApplicationSettings {
    /// Formats application settings for display.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "    host: {}", self.host)?;
        writeln!(f, "    port: {}", self.port)?;
        writeln!(f, "    internal_port: {}", self.internal_port)?;
        writeln!(f, "    internal_tls: {}", self.internal_tls.is_some())
    }
}

/// Encryption key configuration with identifier and key material.
#[derive(Debug, Clone, Deserialize)]
pub struct EncryptionKeyConfig {
    /// Unique identifier for the key.
    pub id: u32,
    /// Base64-encoded key material.
    pub key: String,
}

/// Errors that can occur during API key validation and conversion.
#[derive(Debug, Error)]
pub enum ApiKeyConversionError {
    /// The API key is not valid base64.
    #[error("API key is not base64 encoded")]
    NotBase64Encoded,

    /// The API key does not have the expected length of 32 bytes.
    #[error("Expected length of API key is 32, but actual length is {0}")]
    LengthNot32Bytes(usize),
}

/// Validated API key as a 32-byte array.
///
/// Ensures API keys meet length requirements and are properly decoded from
/// base64.
#[derive(Debug)]
pub struct ApiKey {
    /// The 32-byte decoded API key.
    pub key: [u8; API_KEY_LENGTH_IN_BYTES],
}

impl TryFrom<&str> for ApiKey {
    type Error = ApiKeyConversionError;

    /// Creates an [`ApiKey`] from a base64-encoded string.
    ///
    /// Validates that the string is valid base64 and decodes to exactly 32
    /// bytes.
    ///
    /// # Panics
    /// Panics if the decoded key cannot be converted to a 32-byte array.
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let key =
            BASE64_STANDARD.decode(value).map_err(|_| ApiKeyConversionError::NotBase64Encoded)?;

        if key.len() != API_KEY_LENGTH_IN_BYTES {
            return Err(ApiKeyConversionError::LengthNot32Bytes(key.len()));
        }

        Ok(ApiKey { key: key.try_into().expect("failed to convert api key into array") })
    }
}

impl<'de> Deserialize<'de> for ApiKey {
    /// Deserializes an [`ApiKey`] from configuration.
    ///
    /// Expects a struct with a base64-encoded `key` field that decodes to 32
    /// bytes.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Key,
        }

        struct ApiKeyVisitor;

        impl<'de> Visitor<'de> for ApiKeyVisitor {
            type Value = ApiKey;

            /// Returns the expected input format description.
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct ApiKey")
            }

            /// Visits a map to deserialize an [`ApiKey`].
            fn visit_map<V>(self, mut map: V) -> Result<ApiKey, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut key: Option<&str> = None;
                while let Some(map_key) = map.next_key()? {
                    match map_key {
                        Field::Key => {
                            if key.is_some() {
                                return Err(de::Error::duplicate_field("key"));
                            }
                            key = Some(map.next_value()?);
                        }
                    }
                }
                let key_str = key.ok_or_else(|| de::Error::missing_field("key"))?;
                let key = key_str.try_into().map_err(|_| {
                    de::Error::invalid_value(
                        de::Unexpected::Str(key_str),
                        &"base64 encoded 32 bytes",
                    )
                })?;
                Ok(key)
            }
        }

        const FIELDS: &[&str] = &["key"];
        deserializer.deserialize_struct("ApiKey", FIELDS, ApiKeyVisitor)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn internal_listener_tls_configuration_is_optional() {
        let plaintext: ApplicationSettings = serde_json::from_value(json!({
            "host": "127.0.0.1",
            "port": 8080,
            "internal_port": 8081
        }))
        .unwrap();
        assert!(plaintext.internal_tls.is_none());

        let tls: ApplicationSettings = serde_json::from_value(json!({
            "host": "127.0.0.1",
            "port": 8080,
            "internal_port": 8081,
            "internal_tls": {
                "cert_path": "/etc/etl-api/internal-tls/tls.crt",
                "key_path": "/etc/etl-api/internal-tls/tls.key"
            }
        }))
        .unwrap();
        let tls = tls.internal_tls.unwrap();
        assert_eq!(tls.cert_path, PathBuf::from("/etc/etl-api/internal-tls/tls.crt"));
        assert_eq!(tls.key_path, PathBuf::from("/etc/etl-api/internal-tls/tls.key"));
    }

    #[test]
    fn removed_destination_replicator_defaults_are_ignored() {
        let config: K8sConfig = serde_json::from_value(json!({
            "replicator_resources": {
                "memory_request_mib": 2000,
                "cpu_request_millicores": 500,
                "destinations": {
                    "ducklake": {
                        "memory_request_mib": 4000
                    }
                }
            },
            "vector_resources": {
                "memory_request_mib": 192,
                "cpu_request_millicores": 75
            }
        }))
        .unwrap();

        assert_eq!(config.replicator_resources.memory_request_mib, 2000);
        assert_eq!(config.replicator_resources.cpu_request_millicores, 500);
        assert_eq!(config.replicator_autoscaling, None);
    }

    #[test]
    fn ducklake_copy_buffer_default_is_configurable() {
        #[derive(serde::Deserialize)]
        struct ConfigFragment {
            replicator: ApiReplicatorConfig,
        }

        let config: ConfigFragment = serde_json::from_value(json!({
            "replicator": {
                "destination_defaults": {
                    "ducklake": {
                        "copy_buffer": {
                            "enabled": true,
                            "target_bytes": 268435456,
                            "max_total_bytes": 1073741824
                        }
                    }
                }
            }
        }))
        .unwrap();

        assert_eq!(
            config.replicator.destination_defaults.ducklake.copy_buffer,
            DuckLakeCopyBufferConfig {
                enabled: true,
                target_bytes: 256 * 1024 * 1024,
                max_total_bytes: 1024 * 1024 * 1024,
            }
        );
    }

    #[test]
    fn ducklake_copy_buffer_is_enabled_by_default() {
        let config: ApiReplicatorConfig = serde_json::from_value(json!({})).unwrap();

        assert!(config.destination_defaults.ducklake.copy_buffer.enabled);
    }

    #[test]
    fn replicator_autoscaling_bounds_must_be_positive_and_ordered() {
        let invalid = ReplicatorResourceAutoscalingConfig {
            initial_update_mode: ReplicatorResourceAutoscalingUpdateMode::Off,
            min_memory_mib: 1024,
            max_memory_mib: 512,
            min_cpu_millicores: 300,
            max_cpu_millicores: 200,
        };

        assert!(invalid.validate().unwrap_err().contains("minimum memory"));

        let invalid = ReplicatorResourceAutoscalingConfig {
            initial_update_mode: ReplicatorResourceAutoscalingUpdateMode::Off,
            min_memory_mib: 0,
            max_memory_mib: 8_192,
            min_cpu_millicores: 250,
            max_cpu_millicores: 2_000,
        };
        assert!(invalid.validate().unwrap_err().contains("must be greater than 0"));
    }

    #[test]
    fn replicator_autoscaling_initial_update_mode_is_configurable() {
        let modes = [
            ("off", ReplicatorResourceAutoscalingUpdateMode::Off, "Off"),
            ("initial", ReplicatorResourceAutoscalingUpdateMode::Initial, "Initial"),
            ("recreate", ReplicatorResourceAutoscalingUpdateMode::Recreate, "Recreate"),
            (
                "in_place_or_recreate",
                ReplicatorResourceAutoscalingUpdateMode::InPlaceOrRecreate,
                "InPlaceOrRecreate",
            ),
            ("in_place", ReplicatorResourceAutoscalingUpdateMode::InPlace, "InPlace"),
        ];

        for (configured_mode, expected_mode, k8s_mode) in modes {
            let config: ReplicatorResourceAutoscalingConfig = serde_json::from_value(json!({
                "initial_update_mode": configured_mode,
                "min_memory_mib": 768,
                "max_memory_mib": 8192,
                "min_cpu_millicores": 250,
                "max_cpu_millicores": 2000
            }))
            .unwrap();

            assert_eq!(config.initial_update_mode, expected_mode);
            assert_eq!(config.initial_update_mode.as_k8s_value(), k8s_mode);
        }
    }

    #[test]
    fn replicator_scheduling_constraints_are_optional_and_independent() {
        let unpinned: K8sConfig = serde_json::from_value(json!({
            "replicator_resources": {
                "memory_request_mib": 2000,
                "cpu_request_millicores": 500
            },
            "vector_resources": {
                "memory_request_mib": 192,
                "cpu_request_millicores": 75
            }
        }))
        .unwrap();
        assert_eq!(unpinned.replicator_namespace, "etl-data-plane");
        assert_eq!(unpinned.replicator_service_account_name, "etl-replicator");
        assert!(unpinned.replicator_node_selectors.is_empty());
        assert!(unpinned.replicator_tolerations.is_empty());
        assert_eq!(unpinned.vector_image, "timberio/vector:0.55.0-distroless-libc");

        let configured: K8sConfig = serde_json::from_value(json!({
            "replicator_namespace": "custom-data-plane",
            "replicator_service_account_name": "custom-replicator",
            "replicator_node_selectors": [{
                "key": "example.com/node-pool",
                "value": "data"
            }, {
                "key": "kubernetes.io/arch",
                "value": "arm64"
            }],
            "replicator_tolerations": [{
                "key": "example.com/dedicated",
                "value": "analytics",
                "effect": "CustomEffect"
            }],
            "vector_image": "example.com/vector:custom",
            "replicator_resources": {
                "memory_request_mib": 2000,
                "cpu_request_millicores": 500
            },
            "vector_resources": {
                "memory_request_mib": 192,
                "cpu_request_millicores": 75
            }
        }))
        .unwrap();

        assert_eq!(configured.replicator_namespace, "custom-data-plane");
        assert_eq!(configured.replicator_service_account_name, "custom-replicator");
        assert_eq!(configured.replicator_node_selectors[0].key, "example.com/node-pool");
        assert_eq!(configured.replicator_node_selectors[0].value, "data");
        assert_eq!(configured.replicator_node_selectors[1].key, "kubernetes.io/arch");
        assert_eq!(configured.replicator_node_selectors[1].value, "arm64");
        assert_eq!(configured.replicator_tolerations[0].key, "example.com/dedicated");
        assert_eq!(configured.replicator_tolerations[0].value, "analytics");
        assert_eq!(configured.replicator_tolerations[0].effect, "CustomEffect");
        assert_eq!(configured.vector_image, "example.com/vector:custom");
    }
}
