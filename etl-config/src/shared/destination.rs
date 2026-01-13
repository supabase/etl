use secrecy::SecretString;
use serde::{Deserialize, Serialize};

fn default_max_concurrent_streams() -> usize {
    DestinationConfig::DEFAULT_MAX_CONCURRENT_STREAMS
}

/// Configuration for supported ETL data destinations.
///
/// Specifies the destination type and its associated configuration parameters.
/// Each variant corresponds to a different supported destination system.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets in the config into serialized forms.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationConfig {
    /// In-memory destination for ephemeral or test data.
    Memory,
    /// Google BigQuery destination configuration.
    ///
    /// Use this variant to configure a BigQuery destination, including
    /// project and dataset identifiers, service account credentials, and
    /// optional staleness settings.
    BigQuery {
        /// Google Cloud project identifier.
        project_id: String,
        /// BigQuery dataset identifier.
        dataset_id: String,
        /// Service account key for authenticating with BigQuery.
        service_account_key: SecretString,
        /// Maximum staleness in minutes for BigQuery CDC reads.
        ///
        /// If not set, the default staleness behavior is used. See
        /// <https://cloud.google.com/bigquery/docs/change-data-capture#create-max-staleness>.
        max_staleness_mins: Option<u16>,
        /// Maximum number of concurrent streams for BigQuery append operations.
        ///
        /// Defines the upper limit of concurrent streams used for a **single** append
        /// request to BigQuery.
        ///
        /// This does not limit the total number of streams across the entire system.
        /// The actual number of streams in use at any given time depends on:
        /// - the number of tables being replicated,
        /// - the volume of events processed by the ETL,
        /// - and the configured batch size.
        #[serde(default = "default_max_concurrent_streams")]
        max_concurrent_streams: usize,
    },
    Iceberg {
        #[serde(flatten)]
        config: IcebergConfig,
    },
}

impl DestinationConfig {
    /// Default maximum number of concurrent streams for BigQuery destinations.
    pub const DEFAULT_MAX_CONCURRENT_STREAMS: usize = 8;
}

/// Configuration for the iceberg destination with two variants
///
/// 1. Supabase - for analytics buckets on Supabase
/// 2. Rest - for other REST catalogs.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets in the config into serialized forms.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IcebergConfig {
    Supabase {
        /// Supabase project_ref
        project_ref: String,
        /// Name of the warehouse in the catalog
        warehouse_name: String,
        /// If present, the iceberg catalog namespace where tables will be created.
        /// If missing, multiple catlog namespaces will be created, one per source
        /// schema.
        namespace: Option<String>,
        /// Catalog authentication token
        catalog_token: SecretString,
        /// The S3 access key id
        s3_access_key_id: SecretString,
        /// The S3 secret access key
        s3_secret_access_key: SecretString,
        /// The S3 region
        s3_region: String,
    },
    Rest {
        /// Iceberg catalog uri
        catalog_uri: String,
        /// Name of the warehouse in the catalog
        warehouse_name: String,
        /// If present, the iceberg catalog namespace where tables will be created.
        /// If missing, multiple catlog namespaces will be created, one per source
        /// schema.
        namespace: Option<String>,
        /// The S3 access key id
        s3_access_key_id: SecretString,
        /// The S3 secret access key
        s3_secret_access_key: SecretString,
        /// The S3 endpoint
        s3_endpoint: String,
    },
}

/// Same as [`IcebergConfig`] but without secrets. This type
/// implements [`Serialize`] because it does not contains secrets
/// so is safe to serialize.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IcebergConfigWithoutSecrets {
    Supabase {
        /// Supabase project_ref
        project_ref: String,
        /// Name of the warehouse in the catalog
        warehouse_name: String,
        /// If present, the iceberg catalog namespace where tables will be created.
        /// If missing, multiple catlog namespaces will be created, one per source
        /// schema.
        namespace: Option<String>,
        /// The S3 region
        s3_region: String,
    },
    Rest {
        /// Iceberg catalog uri
        catalog_uri: String,
        /// Name of the warehouse in the catalog
        warehouse_name: String,
        /// Iceberg catalog namespace where tables will be created
        namespace: Option<String>,
        /// The S3 endpoint
        s3_endpoint: String,
    },
}

impl From<IcebergConfig> for IcebergConfigWithoutSecrets {
    fn from(value: IcebergConfig) -> Self {
        match value {
            IcebergConfig::Supabase {
                project_ref,
                warehouse_name,
                namespace,
                catalog_token: _,
                s3_access_key_id: _,
                s3_secret_access_key: _,
                s3_region,
            } => IcebergConfigWithoutSecrets::Supabase {
                project_ref,
                warehouse_name,
                namespace,
                s3_region,
            },
            IcebergConfig::Rest {
                catalog_uri,
                warehouse_name,
                namespace,
                s3_access_key_id: _,
                s3_secret_access_key: _,
                s3_endpoint,
            } => IcebergConfigWithoutSecrets::Rest {
                catalog_uri,
                warehouse_name,
                namespace,
                s3_endpoint,
            },
        }
    }
}

/// Same as [`DestinationConfig`] but without secrets. This type
/// implements [`Serialize`] because it does not contains secrets
/// so is safe to serialize.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationConfigWithoutSecrets {
    /// In-memory destination for ephemeral or test data.
    Memory,
    /// Google BigQuery destination configuration.
    ///
    /// Use this variant to configure a BigQuery destination, including
    /// project and dataset identifiers, service account credentials, and
    /// optional staleness settings.
    BigQuery {
        /// Google Cloud project identifier.
        project_id: String,
        /// BigQuery dataset identifier.
        dataset_id: String,
        /// Maximum staleness in minutes for BigQuery CDC reads.
        ///
        /// If not set, the default staleness behavior is used. See
        /// <https://cloud.google.com/bigquery/docs/change-data-capture#create-max-staleness>.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
        /// Maximum number of concurrent streams for BigQuery append operations.
        ///
        /// Defines the upper limit of concurrent streams used for a **single** append
        /// request to BigQuery.
        ///
        /// This does not limit the total number of streams across the entire system.
        /// The actual number of streams in use at any given time depends on:
        /// - the number of tables being replicated,
        /// - the volume of events processed by the ETL,
        /// - and the configured batch size.
        #[serde(default = "default_max_concurrent_streams")]
        max_concurrent_streams: usize,
    },
    Iceberg {
        #[serde(flatten)]
        config: IcebergConfigWithoutSecrets,
    },
}

impl From<DestinationConfig> for DestinationConfigWithoutSecrets {
    fn from(value: DestinationConfig) -> Self {
        match value {
            DestinationConfig::Memory => DestinationConfigWithoutSecrets::Memory,
            DestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key: _,
                max_staleness_mins,
                max_concurrent_streams,
            } => DestinationConfigWithoutSecrets::BigQuery {
                project_id,
                dataset_id,
                max_staleness_mins,
                max_concurrent_streams,
            },
            DestinationConfig::Iceberg { config } => DestinationConfigWithoutSecrets::Iceberg {
                config: config.into(),
            },
        }
    }
}
