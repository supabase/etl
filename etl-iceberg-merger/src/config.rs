use anyhow::Context;
use etl_config::Config;
use serde::{Deserialize, Serialize};
use std::time::Duration;

type Secret<T> = T;

/// Configuration for the Iceberg merger service.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergerConfig {
    /// Project reference identifier.
    pub project_ref: String,

    /// Environment name (production, staging, development).
    pub environment: String,

    /// Iceberg catalog configuration.
    pub iceberg: IcebergCatalogConfig,

    /// List of changelog tables to process.
    pub tables: Vec<TableConfig>,

    /// Interval between merge runs.
    #[serde(with = "humantime_serde")]
    pub merge_interval: Duration,

    /// Batch size for processing records.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Sentry configuration for error reporting.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sentry: Option<SentryConfig>,
}

/// Configuration for connecting to Iceberg catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergCatalogConfig {
    /// Catalog REST API URL.
    pub catalog_url: String,

    /// Authentication token for the catalog.
    pub catalog_token: Secret<String>,

    /// Warehouse name.
    pub warehouse: String,

    /// S3 endpoint URL.
    pub s3_endpoint: String,

    /// S3 access key ID.
    pub s3_access_key_id: Secret<String>,

    /// S3 secret access key.
    pub s3_secret_access_key: Secret<String>,

    /// S3 region.
    pub s3_region: String,
}

/// Configuration for a changelog table to merge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    /// Namespace (database/schema) name.
    pub namespace: String,

    /// Changelog table name.
    pub changelog_table: String,

    /// Mirror table name (will be created if it doesn't exist).
    pub mirror_table: String,

    /// Primary key column names.
    pub primary_keys: Vec<String>,

    /// CDC sequence number column name.
    #[serde(default = "default_sequence_column")]
    pub sequence_column: String,

    /// CDC operation type column name.
    #[serde(default = "default_operation_column")]
    pub operation_column: String,
}

/// Sentry configuration for error reporting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentryConfig {
    /// Sentry DSN.
    pub dsn: Secret<String>,
}

fn default_batch_size() -> usize {
    10_000
}

fn default_sequence_column() -> String {
    "cdc_sequence_number".to_string()
}

fn default_operation_column() -> String {
    "cdc_operation".to_string()
}

impl MergerConfig {
    /// Validates the merger configuration.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.tables.is_empty() {
            anyhow::bail!("at least one table must be configured");
        }

        for table in &self.tables {
            table.validate()?;
        }

        if self.batch_size == 0 {
            anyhow::bail!("batch_size must be greater than 0");
        }

        Ok(())
    }
}

impl Config for MergerConfig {
    const LIST_PARSE_KEYS: &'static [&'static str] = &[];
}

impl TableConfig {
    /// Validates the table configuration.
    fn validate(&self) -> anyhow::Result<()> {
        if self.namespace.is_empty() {
            anyhow::bail!("namespace cannot be empty");
        }

        if self.changelog_table.is_empty() {
            anyhow::bail!("changelog_table cannot be empty");
        }

        if self.mirror_table.is_empty() {
            anyhow::bail!("mirror_table cannot be empty");
        }

        if self.primary_keys.is_empty() {
            anyhow::bail!("at least one primary key must be specified");
        }

        if self.sequence_column.is_empty() {
            anyhow::bail!("sequence_column cannot be empty");
        }

        if self.operation_column.is_empty() {
            anyhow::bail!("operation_column cannot be empty");
        }

        Ok(())
    }
}

/// Loads and validates the merger configuration.
///
/// Uses the standard configuration loading mechanism from [`etl_config`] and
/// validates the resulting [`MergerConfig`] before returning it.
pub fn load_merger_config() -> anyhow::Result<MergerConfig> {
    let config =
        etl_config::load_config::<MergerConfig>().context("loading merger configuration")?;
    config.validate()?;

    Ok(config)
}
