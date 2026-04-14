use secrecy::SecretString;
use serde::{Deserialize, Serialize};

const fn default_connection_pool_size() -> usize {
    DestinationConfig::DEFAULT_CONNECTION_POOL_SIZE
}

const fn default_ducklake_pool_size() -> u32 {
    DestinationConfig::DEFAULT_DUCKLAKE_POOL_SIZE
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
        /// Size of the BigQuery Storage Write API connection pool.
        ///
        /// Controls the number of concurrent connections maintained in the pool
        /// for writing to BigQuery. The maximum number of inflight requests is
        /// calculated as `connection_pool_size * 100`.
        ///
        /// A higher connection pool size allows more parallel writes but consumes more resources.
        #[serde(default = "default_connection_pool_size")]
        connection_pool_size: usize,
    },
    Iceberg {
        #[serde(flatten)]
        config: IcebergConfig,
    },
    Ducklake {
        /// DuckLake catalog URL.
        catalog_url: String,
        /// DuckLake data path.
        data_path: String,
        /// Size of the DuckDB connection pool.
        #[serde(default = "default_ducklake_pool_size")]
        pool_size: u32,
        /// Optional S3-compatible storage access key ID.
        s3_access_key_id: Option<SecretString>,
        /// Optional S3-compatible storage secret access key.
        s3_secret_access_key: Option<SecretString>,
        /// Optional S3-compatible storage region.
        s3_region: Option<String>,
        /// Optional S3-compatible storage endpoint.
        s3_endpoint: Option<String>,
        /// Optional S3 URL style.
        s3_url_style: Option<String>,
        /// Optional S3 SSL toggle.
        s3_use_ssl: Option<bool>,
        /// Optional metadata schema for DuckLake metadata tables.
        metadata_schema: Option<String>,
    },
}

impl DestinationConfig {
    /// Default connection pool size for BigQuery destinations.
    pub const DEFAULT_CONNECTION_POOL_SIZE: usize = 4;
    /// Default connection pool size for DuckLake destinations.
    pub const DEFAULT_DUCKLAKE_POOL_SIZE: u32 = 4;
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
        /// Size of the BigQuery Storage Write API connection pool.
        ///
        /// Controls the number of concurrent connections maintained in the pool
        /// for writing to BigQuery. The maximum number of inflight requests is
        /// calculated as `connection_pool_size * 100`.
        ///
        /// A higher connection pool size allows more parallel writes but consumes more resources.
        #[serde(default = "default_connection_pool_size")]
        connection_pool_size: usize,
    },
    Iceberg {
        #[serde(flatten)]
        config: IcebergConfigWithoutSecrets,
    },
    Ducklake {
        /// DuckLake catalog URL.
        catalog_url: String,
        /// DuckLake data path.
        data_path: String,
        /// Size of the DuckDB connection pool.
        #[serde(default = "default_ducklake_pool_size")]
        pool_size: u32,
        /// Optional S3-compatible storage region.
        s3_region: Option<String>,
        /// Optional S3-compatible storage endpoint.
        s3_endpoint: Option<String>,
        /// Optional S3 URL style.
        s3_url_style: Option<String>,
        /// Optional S3 SSL toggle.
        s3_use_ssl: Option<bool>,
        /// Optional metadata schema for DuckLake metadata tables.
        metadata_schema: Option<String>,
    },
}

impl From<DestinationConfig> for DestinationConfigWithoutSecrets {
    fn from(value: DestinationConfig) -> Self {
        match value {
            DestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key: _,
                max_staleness_mins,
                connection_pool_size,
            } => DestinationConfigWithoutSecrets::BigQuery {
                project_id,
                dataset_id,
                max_staleness_mins,
                connection_pool_size,
            },
            DestinationConfig::Iceberg { config } => DestinationConfigWithoutSecrets::Iceberg {
                config: config.into(),
            },
            DestinationConfig::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id: _,
                s3_secret_access_key: _,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
            } => DestinationConfigWithoutSecrets::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
            },
        }
    }
}
