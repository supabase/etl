use serde::{Deserialize, Serialize};

use crate::{
    Config,
    shared::{
        DestinationConfig, DestinationConfigWithoutSecrets, PipelineConfigWithoutSecrets,
        SentryConfig, SupabaseConfig, SupabaseConfigWithoutSecrets, Validate, ValidationError,
        pipeline::PipelineConfig,
    },
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
    /// If provided, enables Sentry error reporting and performance monitoring.
    /// If `None`, the replicator operates without Sentry integration.
    pub sentry: Option<SentryConfig>,
    /// Optional Supabase-specific configuration.
    ///
    /// If provided, enables Supabase-specific features or reporting. If `None`,
    /// the replicator operates independently of Supabase.
    pub supabase: Option<SupabaseConfig>,
}

impl ReplicatorConfig {
    /// Returns a reference to the project ref of this configuration.
    pub fn project_ref(&self) -> Option<&str> {
        self.supabase.as_ref().map(|s| s.project_ref.as_ref())
    }
}

impl Validate for ReplicatorConfig {
    /// Validates the complete replicator configuration.
    fn validate(&self) -> Result<(), ValidationError> {
        self.destination.validate()?;
        self.pipeline.validate()?;

        if let Some(sentry) = &self.sentry {
            sentry.validate()?;
        }
        if let Some(supabase) = &self.supabase {
            supabase.validate()?;
        }

        Ok(())
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
    /// If provided, enables Supabase-specific features or reporting. If `None`,
    /// the replicator operates independently of Supabase.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supabase: Option<SupabaseConfigWithoutSecrets>,
}

impl Validate for ReplicatorConfigWithoutSecrets {
    /// Validates the complete without-secret replicator configuration.
    fn validate(&self) -> Result<(), ValidationError> {
        self.destination.validate()?;
        self.pipeline.validate()?;

        if let Some(supabase) = &self.supabase {
            supabase.validate()?;
        }

        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::{
        BatchConfig, BigQueryTableOptions, BigQueryTableOptionsConfig, InvalidatedSlotBehavior,
        MemoryBackpressureConfig, PgConnectionConfig, TableSyncCopyConfig, TcpKeepaliveConfig,
        TlsConfig,
    };

    fn pipeline_config() -> PipelineConfig {
        PipelineConfig {
            id: 1,
            publication_name: "example_publication".to_owned(),
            pg_connection: PgConnectionConfig {
                host: "example.com".to_owned(),
                hostaddr: None,
                port: 5432,
                name: "postgres".to_owned(),
                username: "postgres".to_owned(),
                password: None,
                tls: TlsConfig::disabled(),
                keepalive: TcpKeepaliveConfig::default(),
            },
            store_pg_connection: None,
            batch: BatchConfig::default(),
            table_error_retry_delay_ms: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS,
            table_error_retry_max_attempts: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS,
            max_table_sync_workers: PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS,
            max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
            memory_refresh_interval_ms: PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS,
            table_sync_monitor_refresh_interval_ms:
                PipelineConfig::DEFAULT_TABLE_SYNC_MONITOR_REFRESH_INTERVAL_MS,
            memory_backpressure: Some(MemoryBackpressureConfig::default()),
            table_sync_copy: TableSyncCopyConfig::default(),
            invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
            run_source_migrations: true,
        }
    }

    #[test]
    fn replicator_validation_recurses_with_and_without_secrets() {
        let config = ReplicatorConfig {
            destination: DestinationConfig::BigQuery {
                project_id: "example-project".to_owned(),
                dataset_id: "example_dataset".to_owned(),
                service_account_key: "fake-service-account-key".to_owned().into(),
                max_staleness_mins: None,
                connection_pool_size: DestinationConfig::DEFAULT_CONNECTION_POOL_SIZE,
                table_options: BigQueryTableOptionsConfig {
                    tables: vec![BigQueryTableOptions {
                        table_id: 1,
                        partition_by: None,
                        cluster_by: Vec::new(),
                    }],
                },
            },
            pipeline: pipeline_config(),
            sentry: None,
            supabase: None,
        };
        let without_secrets = ReplicatorConfigWithoutSecrets::from(config.clone());

        assert_eq!(
            config.validate().unwrap_err().to_string(),
            without_secrets.validate().unwrap_err().to_string()
        );
    }
}
