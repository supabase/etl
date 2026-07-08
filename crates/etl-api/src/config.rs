use std::fmt;

use base64::{Engine, prelude::BASE64_STANDARD};
use etl_config::{
    Config,
    shared::{PgConnectionConfig, SentryConfig, TlsConfig},
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
    /// Source database configuration and validation settings.
    pub source: SourceConfig,
    /// Encryption key configurations.
    pub encryption_keys: Vec<EncryptionKeyConfig>,
    /// List of base64-encoded API keys.
    ///
    /// All keys in this list are considered valid for authentication.
    pub api_keys: Vec<String>,
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

/// Kubernetes-specific API configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct K8sConfig {
    /// Default request sizing for replicator workloads.
    ///
    /// This key remains `replicator_resources` in API configuration files. It
    /// provides the mandatory baseline CPU and memory requests used for every
    /// replicator pod unless a pipeline-level override supplies one of those
    /// request values.
    pub replicator_resources: DefaultReplicationResourcesConfig,
}

/// Mandatory default request sizing for replicator workloads.
///
/// These values are part of the ETL API service configuration, not the
/// customer-facing pipeline configuration. They define the baseline Kubernetes
/// requests for replicator containers. Resource limits are not configurable
/// here; the ETL API derives them from requests with its static multipliers
/// unless a pipeline-level override provides explicit limits.
#[derive(Debug, Clone, Deserialize)]
pub struct DefaultReplicationResourcesConfig {
    /// Replicator memory request, in Mi.
    pub replicator_memory_request_mib: i32,
    /// Replicator CPU request, in millicores.
    pub replicator_cpu_request_millicores: i32,
}

impl ApiConfig {
    /// Validates API service configuration.
    pub fn validate(&self) -> Result<(), String> {
        self.k8s.replicator_resources.validate()
    }
}

impl DefaultReplicationResourcesConfig {
    /// Validates that configured request values are positive.
    pub fn validate(&self) -> Result<(), String> {
        validate_positive_request(
            "K8s replicator memory request",
            self.replicator_memory_request_mib,
        )?;
        validate_positive_request(
            "K8s replicator cpu request",
            self.replicator_cpu_request_millicores,
        )?;

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
    const LIST_PARSE_KEYS: &'static [&'static str] = &["api_keys"];
}

/// HTTP server configuration settings.
#[derive(Debug, Clone, Deserialize)]
pub struct ApplicationSettings {
    /// Host address the API listens on.
    pub host: String,
    /// Port number the API listens on.
    pub port: u16,
}

impl fmt::Display for ApplicationSettings {
    /// Formats application settings for display.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "    host: {}", self.host)?;
        writeln!(f, "    port: {}", self.port)
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
