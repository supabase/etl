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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogConfig {
    /// REST catalog configuration
    Rest {
        /// REST catalog URI
        uri: String,
        /// Warehouse location
        warehouse: String,
        /// Optional authentication credentials
        credentials: Option<RestCredentials>,
    },
    /// SQL catalog configuration
    Sql {
        /// SQL database URI
        uri: String,
        /// Warehouse location
        warehouse: String,
    },
    /// AWS Glue catalog configuration
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageConfig {
    /// Amazon S3 storage
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
    /// Google Cloud Storage
    Gcs {
        /// GCS bucket name
        bucket: String,
        /// Optional prefix for all objects
        prefix: Option<String>,
        /// Service account key JSON
        service_account_key: SerializableSecretString,
    },
    /// Azure Blob Storage
    Azure {
        /// Storage account name
        account: String,
        /// Container name
        container: String,
        /// Optional prefix for all objects
        prefix: Option<String>,
        /// Storage account key
        access_key: SerializableSecretString,
    },
    /// Local filesystem (for testing)
    Local {
        /// Base directory path
        path: String,
    },
}

/// Writer configuration for Iceberg.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterConfig {
    /// Target file size in bytes (default: 512MB)
    #[serde(default = "default_target_file_size")]
    pub target_file_size_bytes: usize,
    
    /// Maximum number of open files (default: 10)
    #[serde(default = "default_max_open_files")]
    pub max_open_files: usize,
    
    /// Compression type for Parquet files
    #[serde(default = "default_compression")]
    pub compression: CompressionType,
    
    /// Batch size for writing (default: 1000)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    
    /// Commit interval in milliseconds (default: 60000)
    #[serde(default = "default_commit_interval_ms")]
    pub commit_interval_ms: u64,
    
    /// Enable metrics collection
    #[serde(default = "default_enable_metrics")]
    pub enable_metrics: bool,
}

/// Compression type for Parquet files.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionType {
    /// No compression
    None,
    /// Snappy compression (default)
    Snappy,
    /// Gzip compression
    Gzip,
    /// LZ4 compression
    Lz4,
    /// Zstd compression
    Zstd,
}

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
            target_file_size_bytes: default_target_file_size(),
            max_open_files: default_max_open_files(),
            compression: default_compression(),
            batch_size: default_batch_size(),
            commit_interval_ms: default_commit_interval_ms(),
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
fn default_target_file_size() -> usize {
    512 * 1024 * 1024 // 512MB
}

fn default_max_open_files() -> usize {
    10
}

fn default_compression() -> CompressionType {
    CompressionType::Snappy
}

fn default_batch_size() -> usize {
    1000
}

fn default_commit_interval_ms() -> u64 {
    60000 // 1 minute
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
        assert_eq!(config.target_file_size_bytes, 512 * 1024 * 1024);
        assert_eq!(config.max_open_files, 10);
        assert!(matches!(config.compression, CompressionType::Snappy));
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.commit_interval_ms, 60000);
    }

    #[test]
    fn test_default_cdc_config() {
        let config = CdcConfig::default();
        assert!(config.enable_deletes);
        assert!(config.track_changes);
        assert!(config.add_lsn_column);
        assert!(config.add_timestamp_column);
    }
}