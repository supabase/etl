use secrecy::SecretString;
use serde::{Deserialize, Serialize};

/// Supabase integration configuration.
///
/// Contains Supabase-specific settings for ETL applications that
/// integrate with Supabase services.
#[derive(Debug, Clone, Deserialize)]
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
    /// Optional ConfigCat SDK key for feature flag integration used at Supabase.
    ///
    /// If provided, enables ConfigCat feature flag evaluation.
    /// If `None`, the replicator operates without feature flag support.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configcat_sdk_key: Option<String>,
}

/// Supabase integration configuration without secrets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupabaseConfigWithoutSecrets {
    /// Supabase project reference identifier.
    pub project_ref: String,
    /// Supabase API URL for error notifications.
    ///
    /// This URL is used to send error notifications to the Supabase API.
    /// If not provided, error notifications will not be sent.
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
