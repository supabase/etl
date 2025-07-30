use crate::shared::pipeline::PipelineConfig;
use crate::shared::{DestinationConfig, SentryConfig, SupabaseConfig, ValidationError};
use serde::{Deserialize, Serialize};

/// Configuration for the replicator service.
///
/// This struct aggregates all configuration required to run the replicator, including source,
/// destination, pipeline, state store, and optional Supabase-specific settings.
///
/// The [`ReplicatorConfig`] is typically deserialized from a configuration file and passed to the
/// replicator at startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
    /// Validates the loaded [`ReplicatorConfig`].
    ///
    /// Checks the validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] if validation fails.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.pipeline.validate()
    }
}
