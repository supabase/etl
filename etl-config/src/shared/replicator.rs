use crate::shared::pipeline::PipelineConfig;
use crate::shared::{DestinationConfig, SentryConfig, SupabaseConfig, ValidationError};
use serde::{Deserialize, Serialize};

/// Complete configuration for the replicator service.
///
/// Aggregates all configuration required to run a replicator including pipeline
/// settings, destination configuration, and optional service integrations like
/// Sentry and Supabase. Typically loaded from configuration files at startup.
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
    /// Validates the complete replicator configuration.
    ///
    /// Performs comprehensive validation of all configuration components.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.pipeline.validate()
    }
}
