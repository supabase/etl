use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Supabase integration configuration.
///
/// Contains Supabase-specific settings for ETL applications that
/// integrate with Supabase services.
#[derive(Clone, Deserialize)]
pub struct SupabaseConfig {
    /// Supabase project reference identifier.
    pub project_ref: String,
    /// Supabase API URL for error notifications.
    ///
    /// This URL is used to send error notifications to the Supabase API.
    /// If not provided, error notifications will not be sent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_url: Option<String>,
    /// Supabase API key for authentication.
    ///
    /// This API key is used to authenticate requests to the Supabase API.
    /// Required when `api_url` is provided.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<SecretString>,
}

impl fmt::Debug for SupabaseConfig {
    /// Formats the config for debugging with redacted sensitive fields.
    ///
    /// The project reference, API URL, and API key are redacted in debug output for security.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SupabaseConfig")
            .field("project_ref", &"REDACTED")
            .field("api_url", &self.api_url.as_ref().map(|_| "REDACTED"))
            .field("api_key", &self.api_key.as_ref().map(|_| "REDACTED"))
            .finish()
    }
}

/// Same as [`SupabaseConfig`] but without secrets.
///
/// Used when serializing configuration for external systems (like Kubernetes)
/// where secrets should not be included.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupabaseConfigWithoutSecrets {
    /// Supabase project reference identifier.
    pub project_ref: String,
    /// Supabase API URL for error notifications.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_url: Option<String>,
}

impl From<SupabaseConfig> for SupabaseConfigWithoutSecrets {
    fn from(value: SupabaseConfig) -> Self {
        SupabaseConfigWithoutSecrets {
            project_ref: value.project_ref,
            api_url: value.api_url,
        }
    }
}
