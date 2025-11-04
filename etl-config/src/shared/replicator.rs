use serde::{Deserialize, Serialize};

use crate::Config;
use crate::shared::pipeline::PipelineConfig;
use crate::shared::{
    DestinationConfig, DestinationConfigWithoutSecrets, PipelineConfigWithoutSecrets, SentryConfig,
    SupabaseConfig, SupabaseConfigWithoutSecrets, ValidationError,
};

/// Complete configuration for the replicator service.
///
/// Aggregates all configuration required to run a replicator including pipeline
/// settings, destination configuration, and optional service integrations like
/// Sentry and Supabase. Typically loaded from configuration files at startup.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets in the config into serialized forms.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicatorConfig {
    /// Configuration for the replication destination.
    pub destination: DestinationConfig,
    /// Configuration for the replication pipeline.
    pub pipeline: PipelineConfig,
    /// Optional Sentry configuration for error tracking.
    ///
    /// If provided, enables Sentry error reporting and performance monitoring. If `None`, the replicator operates without Sentry integration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sentry: Option<SentryConfig>,
    /// Optional Supabase-specific configuration.
    ///
    /// If provided, enables Supabase-specific features or reporting. If `None`, the replicator operates independently of Supabase.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supabase: Option<SupabaseConfig>,
}

impl ReplicatorConfig {
    /// Validates the complete replicator configuration.
    ///
    /// Performs comprehensive validation of all configuration components.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.pipeline.validate()
    }
}

impl Config for ReplicatorConfig {
    const LIST_PARSE_KEYS: &'static [&'static str] = &[];
}

/// Same as [`ReplicatorConfig`] but without secrets.
///
/// This type implements [`Serialize`] because it does not contain secrets.
/// so is safe to serialize.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatorConfigWithoutSecrets {
    /// Configuration for the replication destination.
    pub destination: DestinationConfigWithoutSecrets,
    /// Configuration for the replication pipeline.
    pub pipeline: PipelineConfigWithoutSecrets,
    /// Optional Supabase-specific configuration.
    ///
    /// If provided, enables Supabase-specific features or reporting. If `None`, the replicator operates independently of Supabase.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supabase: Option<SupabaseConfigWithoutSecrets>,
}

impl From<ReplicatorConfig> for ReplicatorConfigWithoutSecrets {
    fn from(value: ReplicatorConfig) -> Self {
        ReplicatorConfigWithoutSecrets {
            destination: value.destination.into(),
            pipeline: value.pipeline.into(),
            supabase: value.supabase.map(Into::into),
        }
    }
}
