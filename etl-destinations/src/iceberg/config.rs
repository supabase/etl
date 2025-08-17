//! Configuration structures for the Iceberg destination.

use etl_config::SerializableSecretString;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Main configuration for the Iceberg destination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergConfig {
    /// Catalog configuration
    pub catalog: CatalogConfig,

    /// Namespace for Iceberg tables
    pub namespace: String,

    /// Optional table prefix
    pub table_prefix: Option<String>,

    /// Storage configuration
    pub storage: StorageConfig,

    /// Writer configuration
    pub writer_config: WriterConfig,

    /// CDC configuration
    pub cdc_config: CdcConfig,
}

/// Catalog configuration for Iceberg.
/// Focused on AWS-native catalogs with REST catalog support.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogConfig {
    /// REST catalog configuration (AWS S3 Tables, Tabular, etc.)
    Rest {
        /// REST catalog URI
        uri: String,
        /// Warehouse location
        warehouse: String,
        /// Optional authentication credentials
        credentials: Option<RestCredentials>,
    },
    /// AWS Glue catalog configuration (native AWS)
    Glue {
        /// AWS region
        region: String,
        /// Warehouse location
        warehouse: String,
        /// AWS credentials
        credentials: AwsCredentials,
    },
}

/// REST catalog credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestCredentials {
    /// Bearer token for authentication
    pub token: Option<SerializableSecretString>,
    /// OAuth2 configuration
    pub oauth2: Option<OAuth2Config>,
}

/// OAuth2 configuration for REST catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuth2Config {
    /// OAuth2 client ID
    pub client_id: String,
    /// OAuth2 client secret
    pub client_secret: SerializableSecretString,
    /// OAuth2 token endpoint
    pub token_endpoint: String,
}

/// AWS credentials for Glue catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsCredentials {
    /// AWS access key ID
    pub access_key_id: SerializableSecretString,
    /// AWS secret access key
    pub secret_access_key: SerializableSecretString,
    /// Optional session token
    pub session_token: Option<SerializableSecretString>,
}

/// Storage configuration for Iceberg tables.
/// Currently optimized for AWS S3, with extensible design for future cloud providers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageConfig {
    /// Amazon S3 storage (production-ready)
    S3 {
        /// S3 bucket name
        bucket: String,
        /// Optional prefix for all objects
        prefix: Option<String>,
        /// AWS region
        region: String,
        /// AWS credentials (if not using instance profile)
        credentials: Option<AwsCredentials>,
    },
    /// Local filesystem (for testing only)
    Local {
        /// Base directory path
        path: String,
    },
    // Future cloud providers can be added here:
    // Gcs { ... },
    // Azure { ... },
}

/// Writer configuration for Iceberg.
/// Simplified for native Iceberg Writer API - compression and file management handled automatically.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterConfig {
    /// Batch size for writing (default: 1000)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum batch size in bytes (default: 30MB, optimized for S3)
    #[serde(default = "default_max_batch_size_bytes")]
    pub max_batch_size_bytes: usize,

    /// Maximum time to wait before committing (default: 10s for low latency)
    #[serde(default = "default_max_commit_time_ms")]
    pub max_commit_time_ms: u64,

    /// Enable metrics collection
    #[serde(default = "default_enable_metrics")]
    pub enable_metrics: bool,
    // Note: Compression, target file size, and file management are now handled
    // automatically by the native Iceberg Writer API for optimal performance
}

// CompressionType removed - now handled automatically by native Iceberg Writer API

/// CDC (Change Data Capture) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcConfig {
    /// Enable delete operations (default: true)
    #[serde(default = "default_enable_deletes")]
    pub enable_deletes: bool,

    /// Track change types in data (default: true)
    #[serde(default = "default_track_changes")]
    pub track_changes: bool,

    /// Add LSN column for ordering (default: true)
    #[serde(default = "default_add_lsn_column")]
    pub add_lsn_column: bool,

    /// Add timestamp column (default: true)
    #[serde(default = "default_add_timestamp_column")]
    pub add_timestamp_column: bool,
}

/// Default implementations for configuration fields.
impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            max_batch_size_bytes: default_max_batch_size_bytes(),
            max_commit_time_ms: default_max_commit_time_ms(),
            enable_metrics: default_enable_metrics(),
        }
    }
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            enable_deletes: default_enable_deletes(),
            track_changes: default_track_changes(),
            add_lsn_column: default_add_lsn_column(),
            add_timestamp_column: default_add_timestamp_column(),
        }
    }
}

// Default value functions for serde
fn default_batch_size() -> usize {
    1000
}

fn default_max_batch_size_bytes() -> usize {
    30 * 1024 * 1024 // 30MB - optimized for S3 Tables API
}

fn default_max_commit_time_ms() -> u64 {
    10_000 // 10 seconds max latency
}

fn default_enable_metrics() -> bool {
    true
}

fn default_enable_deletes() -> bool {
    true
}

fn default_track_changes() -> bool {
    true
}

fn default_add_lsn_column() -> bool {
    true
}

fn default_add_timestamp_column() -> bool {
    true
}

/// Partition strategy configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStrategy {
    /// No partitioning
    None,
    /// Partition by day
    Daily { column: String },
    /// Partition by month
    Monthly { column: String },
    /// Partition by year
    Year { column: String },
    /// Partition by hour
    Hourly { column: String },
    /// Hash partitioning
    Hash { column: String, buckets: u32 },
    /// Custom partitioning expression
    Custom { expression: String },
}

impl Default for PartitionStrategy {
    fn default() -> Self {
        Self::None
    }
}

/// AWS-specific configuration helpers for optimal setup.
impl IcebergConfig {
    /// Creates an AWS-optimized configuration for S3 Tables.
    ///
    /// This is the recommended setup for AWS S3 Tables with REST catalog.
    pub fn aws_s3_tables(
        rest_catalog_uri: String,
        s3_bucket: String,
        region: String,
        namespace: String,
    ) -> Self {
        Self {
            catalog: CatalogConfig::Rest {
                uri: rest_catalog_uri,
                warehouse: format!("s3://{}/warehouse", s3_bucket),
                credentials: None, // Use IAM roles/instance profiles
            },
            namespace,
            table_prefix: Some("etl_".to_string()),
            storage: StorageConfig::S3 {
                bucket: s3_bucket,
                prefix: Some("data/".to_string()),
                region,
                credentials: None, // Use IAM roles/instance profiles
            },
            writer_config: WriterConfig::aws_optimized(),
            cdc_config: CdcConfig::default(),
        }
    }

    /// Creates an AWS Glue catalog configuration.
    ///
    /// For use with AWS Glue Data Catalog and S3 storage.
    pub fn aws_glue(
        s3_bucket: String,
        region: String,
        namespace: String,
        credentials: AwsCredentials,
    ) -> Self {
        Self {
            catalog: CatalogConfig::Glue {
                region: region.clone(),
                warehouse: format!("s3://{}/warehouse", s3_bucket),
                credentials: credentials.clone(),
            },
            namespace,
            table_prefix: Some("etl_".to_string()),
            storage: StorageConfig::S3 {
                bucket: s3_bucket,
                prefix: Some("data/".to_string()),
                region,
                credentials: Some(credentials),
            },
            writer_config: WriterConfig::aws_optimized(),
            cdc_config: CdcConfig::default(),
        }
    }
}

impl WriterConfig {
    /// Creates AWS-optimized writer configuration.
    ///
    /// Optimized for S3 Tables API with 30MB batches and 10s max commit time for low latency.
    pub fn aws_optimized() -> Self {
        Self {
            batch_size: 1000,
            max_batch_size_bytes: 30 * 1024 * 1024, // 30MB for S3 Tables
            max_commit_time_ms: 10_000,             // 10s max latency
            enable_metrics: true,
        }
    }
}

/// Table-specific configuration overrides.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableConfig {
    /// Override partition strategy for this table
    pub partition_strategy: Option<PartitionStrategy>,

    /// Override sort order for this table
    pub sort_columns: Option<Vec<String>>,

    /// Custom table properties
    pub properties: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_serialization() {
        let config = IcebergConfig {
            catalog: CatalogConfig::Rest {
                uri: "http://localhost:8181".to_string(),
                warehouse: "s3://bucket/warehouse".to_string(),
                credentials: None,
            },
            namespace: "test_namespace".to_string(),
            table_prefix: Some("etl_".to_string()),
            storage: StorageConfig::S3 {
                bucket: "test-bucket".to_string(),
                prefix: Some("data/".to_string()),
                region: "us-east-1".to_string(),
                credentials: None,
            },
            writer_config: WriterConfig::default(),
            cdc_config: CdcConfig::default(),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: IcebergConfig = serde_json::from_str(&json).unwrap();

        match deserialized.catalog {
            CatalogConfig::Rest { uri, .. } => assert_eq!(uri, "http://localhost:8181"),
            _ => panic!("Wrong catalog type"),
        }
    }

    #[test]
    fn test_default_writer_config() {
        let config = WriterConfig::default();
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.max_batch_size_bytes, 30 * 1024 * 1024); // 30MB
        assert_eq!(config.max_commit_time_ms, 10_000); // 10s
        assert!(config.enable_metrics);
    }

    #[test]
    fn test_default_cdc_config() {
        let config = CdcConfig::default();
        assert!(config.enable_deletes);
        assert!(config.track_changes);
        assert!(config.add_lsn_column);
        assert!(config.add_timestamp_column);
    }

    #[test]
    fn test_aws_s3_tables_config() {
        let config = IcebergConfig::aws_s3_tables(
            "https://s3-tables.us-east-1.amazonaws.com".to_string(),
            "my-data-bucket".to_string(),
            "us-east-1".to_string(),
            "production".to_string(),
        );

        match config.catalog {
            CatalogConfig::Rest { uri, warehouse, .. } => {
                assert_eq!(uri, "https://s3-tables.us-east-1.amazonaws.com");
                assert_eq!(warehouse, "s3://my-data-bucket/warehouse");
            }
            _ => panic!("Expected REST catalog"),
        }

        match config.storage {
            StorageConfig::S3 { bucket, region, .. } => {
                assert_eq!(bucket, "my-data-bucket");
                assert_eq!(region, "us-east-1");
            }
            _ => panic!("Expected S3 storage"),
        }

        assert_eq!(config.namespace, "production");
        assert_eq!(config.writer_config.max_batch_size_bytes, 30 * 1024 * 1024);
    }

    #[test]
    fn test_aws_optimized_writer() {
        let config = WriterConfig::aws_optimized();
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.max_batch_size_bytes, 30 * 1024 * 1024);
        assert_eq!(config.max_commit_time_ms, 10_000); // 10s
        assert!(config.enable_metrics);
    }
}
