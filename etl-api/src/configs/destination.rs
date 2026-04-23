use etl_config::{
    SerializableSecretString,
    shared::{DestinationConfig, IcebergConfig},
};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use url::Url;
use utoipa::ToSchema;

use crate::configs::{
    encryption::{
        Decrypt, DecryptionError, Encrypt, EncryptedValue, EncryptionError, EncryptionKey,
        decrypt_text, encrypt_text,
    },
    store::Store,
};

/// Returns the default connection pool size for BigQuery destinations.
pub const fn default_connection_pool_size() -> usize {
    DestinationConfig::DEFAULT_CONNECTION_POOL_SIZE
}

/// Returns the default connection pool size for DuckLake destinations.
pub const fn default_ducklake_pool_size() -> u32 {
    DestinationConfig::DEFAULT_DUCKLAKE_POOL_SIZE
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FullApiDestinationConfig {
    BigQuery {
        #[schema(example = "my-gcp-project")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        project_id: String,
        #[schema(example = "my_dataset")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        dataset_id: String,
        #[schema(example = "{\"type\": \"service_account\", \"project_id\": \"my-project\"}")]
        service_account_key: SerializableSecretString,
        #[schema(example = 15)]
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
        #[schema(example = 8)]
        #[serde(skip_serializing_if = "Option::is_none")]
        connection_pool_size: Option<usize>,
    },
    ClickHouse {
        /// ClickHouse HTTP(S) endpoint URL.
        #[schema(value_type = String, example = "http://test:8123")]
        #[serde(deserialize_with = "crate::utils::trim_http_url")]
        url: Url,
        /// ClickHouse user name
        #[schema(example = "foo")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        user: String,
        /// ClickHouse password (omit for passwordless access)
        password: Option<SerializableSecretString>,
        /// ClickHouse target database
        #[schema(example = "my_db")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        database: String,
    },
    Iceberg {
        #[serde(flatten)]
        config: FullApiIcebergConfig,
    },
    Ducklake {
        #[schema(example = "postgres://user:pass@localhost:5432/ducklake_catalog")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        catalog_url: String,
        #[schema(example = "file:///absolute/path/to/lake_data")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        data_path: String,
        #[schema(example = 4)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pool_size: Option<u32>,
        #[schema(example = "my-access-key")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_secret_string"
        )]
        s3_access_key_id: Option<SerializableSecretString>,
        #[schema(example = "my-secret-key")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_secret_string"
        )]
        s3_secret_access_key: Option<SerializableSecretString>,
        #[schema(example = "us-east-1")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        s3_region: Option<String>,
        #[schema(example = "127.0.0.1:5000/s3")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        s3_endpoint: Option<String>,
        #[schema(example = "path")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        s3_url_style: Option<String>,
        #[schema(example = false)]
        #[serde(skip_serializing_if = "Option::is_none")]
        s3_use_ssl: Option<bool>,
        #[schema(example = "ducklake")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        metadata_schema: Option<String>,
        #[schema(example = "150MB")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        duckdb_memory_cache_limit: Option<String>,
        #[schema(example = "10MB")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        maintenance_target_file_size: Option<String>,
    },
}

impl From<StoredDestinationConfig> for FullApiDestinationConfig {
    fn from(value: StoredDestinationConfig) -> Self {
        match value {
            StoredDestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size,
            } => Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size: Some(connection_pool_size),
            },
            StoredDestinationConfig::ClickHouse { url, user, password, database } => {
                Self::ClickHouse { url, user, password, database }
            }
            StoredDestinationConfig::Iceberg { config } => match config {
                StoredIcebergConfig::Supabase {
                    project_ref,
                    warehouse_name,
                    namespace,
                    catalog_token,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_region,
                } => FullApiDestinationConfig::Iceberg {
                    config: FullApiIcebergConfig::Supabase {
                        project_ref,
                        warehouse_name,
                        namespace,
                        catalog_token,
                        s3_access_key_id,
                        s3_secret_access_key,
                        s3_region,
                    },
                },
                StoredIcebergConfig::Rest {
                    catalog_uri,
                    warehouse_name,
                    namespace,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_endpoint,
                } => FullApiDestinationConfig::Iceberg {
                    config: FullApiIcebergConfig::Rest {
                        catalog_uri,
                        warehouse_name,
                        namespace,
                        s3_endpoint,
                        s3_access_key_id,
                        s3_secret_access_key,
                    },
                },
            },
            StoredDestinationConfig::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                duckdb_memory_cache_limit,
                maintenance_target_file_size,
            } => Self::Ducklake {
                catalog_url,
                data_path,
                pool_size: Some(pool_size),
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                duckdb_memory_cache_limit,
                maintenance_target_file_size,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum StoredDestinationConfig {
    BigQuery {
        project_id: String,
        dataset_id: String,
        service_account_key: SerializableSecretString,
        max_staleness_mins: Option<u16>,
        connection_pool_size: usize,
    },
    ClickHouse {
        url: Url,
        user: String,
        password: Option<SerializableSecretString>,
        database: String,
    },
    Iceberg {
        config: StoredIcebergConfig,
    },
    Ducklake {
        catalog_url: String,
        data_path: String,
        pool_size: u32,
        s3_access_key_id: Option<SerializableSecretString>,
        s3_secret_access_key: Option<SerializableSecretString>,
        s3_region: Option<String>,
        s3_endpoint: Option<String>,
        s3_url_style: Option<String>,
        s3_use_ssl: Option<bool>,
        metadata_schema: Option<String>,
        duckdb_memory_cache_limit: Option<String>,
        maintenance_target_file_size: Option<String>,
    },
}

impl StoredDestinationConfig {
    pub fn into_etl_config(self) -> DestinationConfig {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size,
            } => DestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key: service_account_key.into(),
                max_staleness_mins,
                connection_pool_size,
            },
            Self::ClickHouse { url, user, password, database } => DestinationConfig::ClickHouse {
                url,
                user,
                password: password.map(Into::into),
                database,
            },
            Self::Iceberg { config } => match config {
                StoredIcebergConfig::Supabase {
                    project_ref,
                    warehouse_name,
                    namespace,
                    catalog_token,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_region,
                } => DestinationConfig::Iceberg {
                    config: IcebergConfig::Supabase {
                        project_ref,
                        warehouse_name,
                        namespace,
                        catalog_token: catalog_token.into(),
                        s3_access_key_id: s3_access_key_id.into(),
                        s3_secret_access_key: s3_secret_access_key.into(),
                        s3_region,
                    },
                },
                StoredIcebergConfig::Rest {
                    catalog_uri,
                    warehouse_name,
                    namespace,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_endpoint,
                } => DestinationConfig::Iceberg {
                    config: IcebergConfig::Rest {
                        catalog_uri,
                        warehouse_name,
                        namespace,
                        s3_access_key_id: s3_access_key_id.into(),
                        s3_secret_access_key: s3_secret_access_key.into(),
                        s3_endpoint,
                    },
                },
            },
            Self::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                duckdb_memory_cache_limit,
                maintenance_target_file_size,
            } => DestinationConfig::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id: s3_access_key_id.map(Into::into),
                s3_secret_access_key: s3_secret_access_key.map(Into::into),
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                duckdb_memory_cache_limit,
                maintenance_target_file_size,
            },
        }
    }
}

impl From<FullApiDestinationConfig> for StoredDestinationConfig {
    fn from(value: FullApiDestinationConfig) -> Self {
        match value {
            FullApiDestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size,
            } => Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size: connection_pool_size
                    .unwrap_or(DestinationConfig::DEFAULT_CONNECTION_POOL_SIZE),
            },
            FullApiDestinationConfig::ClickHouse { url, user, password, database } => {
                Self::ClickHouse { url, user, password, database }
            }
            FullApiDestinationConfig::Iceberg { config } => match config {
                FullApiIcebergConfig::Supabase {
                    project_ref,
                    warehouse_name,
                    namespace,
                    catalog_token,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_region,
                } => Self::Iceberg {
                    config: StoredIcebergConfig::Supabase {
                        project_ref,
                        warehouse_name,
                        namespace,
                        catalog_token,
                        s3_access_key_id,
                        s3_secret_access_key,
                        s3_region,
                    },
                },
                FullApiIcebergConfig::Rest {
                    catalog_uri,
                    warehouse_name,
                    namespace,
                    s3_endpoint,
                    s3_access_key_id,
                    s3_secret_access_key,
                } => Self::Iceberg {
                    config: StoredIcebergConfig::Rest {
                        catalog_uri,
                        warehouse_name,
                        namespace,
                        s3_access_key_id,
                        s3_secret_access_key,
                        s3_endpoint,
                    },
                },
            },
            FullApiDestinationConfig::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                duckdb_memory_cache_limit,
                maintenance_target_file_size,
            } => Self::Ducklake {
                catalog_url,
                data_path,
                pool_size: pool_size.unwrap_or(DestinationConfig::DEFAULT_DUCKLAKE_POOL_SIZE),
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                duckdb_memory_cache_limit,
                maintenance_target_file_size,
            },
        }
    }
}

impl Encrypt<EncryptedStoredDestinationConfig> for StoredDestinationConfig {
    fn encrypt(
        self,
        encryption_key: &EncryptionKey,
    ) -> Result<EncryptedStoredDestinationConfig, EncryptionError> {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size,
            } => {
                let encrypted_service_account_key =
                    encrypt_text(service_account_key.expose_secret().to_owned(), encryption_key)?;

                Ok(EncryptedStoredDestinationConfig::BigQuery {
                    project_id,
                    dataset_id,
                    service_account_key: encrypted_service_account_key,
                    max_staleness_mins,
                    connection_pool_size,
                })
            }
            Self::ClickHouse { url, user, password, database } => {
                let encrypted_password = match password {
                    Some(p) => Some(encrypt_text(p.expose_secret().to_owned(), encryption_key)?),
                    None => None,
                };

                Ok(EncryptedStoredDestinationConfig::ClickHouse {
                    url,
                    user,
                    password: encrypted_password,
                    database,
                })
            }
            Self::Iceberg { config } => match config {
                StoredIcebergConfig::Supabase {
                    project_ref,
                    warehouse_name,
                    namespace,
                    catalog_token,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_region,
                } => {
                    let encrypted_catalog_token =
                        encrypt_text(catalog_token.expose_secret().to_owned(), encryption_key)?;
                    let encrypted_s3_access_key_id =
                        encrypt_text(s3_access_key_id.expose_secret().to_owned(), encryption_key)?;
                    let encrypted_s3_secret_access_key = encrypt_text(
                        s3_secret_access_key.expose_secret().to_owned(),
                        encryption_key,
                    )?;
                    Ok(EncryptedStoredDestinationConfig::Iceberg {
                        config: EncryptedStoredIcebergConfig::Supabase {
                            project_ref,
                            warehouse_name,
                            namespace,
                            catalog_token: encrypted_catalog_token,
                            s3_access_key_id: encrypted_s3_access_key_id,
                            s3_secret_access_key: encrypted_s3_secret_access_key,
                            s3_region,
                        },
                    })
                }
                StoredIcebergConfig::Rest {
                    catalog_uri,
                    warehouse_name,
                    namespace,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_endpoint,
                } => {
                    let encrypted_s3_access_key_id =
                        encrypt_text(s3_access_key_id.expose_secret().to_owned(), encryption_key)?;
                    let encrypted_s3_secret_access_key = encrypt_text(
                        s3_secret_access_key.expose_secret().to_owned(),
                        encryption_key,
                    )?;
                    Ok(EncryptedStoredDestinationConfig::Iceberg {
                        config: EncryptedStoredIcebergConfig::Rest {
                            catalog_uri,
                            warehouse_name,
                            namespace,
                            s3_access_key_id: encrypted_s3_access_key_id,
                            s3_secret_access_key: encrypted_s3_secret_access_key,
                            s3_endpoint,
                        },
                    })
                }
            },
            Self::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                duckdb_memory_cache_limit,
                maintenance_target_file_size,
            } => {
                let s3_access_key_id = s3_access_key_id
                    .map(|value| encrypt_text(value.expose_secret().to_owned(), encryption_key))
                    .transpose()?;
                let s3_secret_access_key = s3_secret_access_key
                    .map(|value| encrypt_text(value.expose_secret().to_owned(), encryption_key))
                    .transpose()?;

                Ok(EncryptedStoredDestinationConfig::Ducklake {
                    catalog_url,
                    data_path,
                    pool_size,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_region,
                    s3_endpoint,
                    s3_url_style,
                    s3_use_ssl,
                    metadata_schema,
                    duckdb_memory_cache_limit,
                    maintenance_target_file_size,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EncryptedStoredDestinationConfig {
    BigQuery {
        project_id: String,
        dataset_id: String,
        service_account_key: EncryptedValue,
        max_staleness_mins: Option<u16>,
        #[serde(default = "default_connection_pool_size")]
        connection_pool_size: usize,
    },
    ClickHouse {
        url: Url,
        user: String,
        password: Option<EncryptedValue>,
        database: String,
    },
    Iceberg {
        #[serde(flatten)]
        config: EncryptedStoredIcebergConfig,
    },
    Ducklake {
        catalog_url: String,
        data_path: String,
        #[serde(default = "default_ducklake_pool_size")]
        pool_size: u32,
        s3_access_key_id: Option<EncryptedValue>,
        s3_secret_access_key: Option<EncryptedValue>,
        s3_region: Option<String>,
        s3_endpoint: Option<String>,
        s3_url_style: Option<String>,
        s3_use_ssl: Option<bool>,
        metadata_schema: Option<String>,
        duckdb_memory_cache_limit: Option<String>,
        maintenance_target_file_size: Option<String>,
    },
}

impl Store for EncryptedStoredDestinationConfig {}

impl Decrypt<StoredDestinationConfig> for EncryptedStoredDestinationConfig {
    fn decrypt(
        self,
        encryption_key: &EncryptionKey,
    ) -> Result<StoredDestinationConfig, DecryptionError> {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key: encrypted_service_account_key,
                max_staleness_mins,
                connection_pool_size,
            } => {
                let service_account_key = SerializableSecretString::from(decrypt_text(
                    encrypted_service_account_key,
                    encryption_key,
                )?);

                Ok(StoredDestinationConfig::BigQuery {
                    project_id,
                    dataset_id,
                    service_account_key,
                    max_staleness_mins,
                    connection_pool_size,
                })
            }
            Self::Iceberg { config } => match config {
                EncryptedStoredIcebergConfig::Supabase {
                    project_ref,
                    warehouse_name,
                    namespace,
                    catalog_token: encrypted_catalog_token,
                    s3_access_key_id: encrypted_s3_access_key_id,
                    s3_secret_access_key: encrypted_s3_secret_access_key,
                    s3_region,
                } => {
                    let catalog_token = SerializableSecretString::from(decrypt_text(
                        encrypted_catalog_token,
                        encryption_key,
                    )?);

                    let s3_access_key_id = SerializableSecretString::from(decrypt_text(
                        encrypted_s3_access_key_id,
                        encryption_key,
                    )?);

                    let s3_secret_access_key = SerializableSecretString::from(decrypt_text(
                        encrypted_s3_secret_access_key,
                        encryption_key,
                    )?);

                    Ok(StoredDestinationConfig::Iceberg {
                        config: StoredIcebergConfig::Supabase {
                            project_ref,
                            warehouse_name,
                            namespace,
                            catalog_token,
                            s3_access_key_id,
                            s3_secret_access_key,
                            s3_region,
                        },
                    })
                }
                EncryptedStoredIcebergConfig::Rest {
                    catalog_uri,
                    warehouse_name,
                    namespace,
                    s3_access_key_id: encrypted_s3_access_key_id,
                    s3_secret_access_key: encrypted_s3_secret_access_key,
                    s3_endpoint,
                } => {
                    let s3_access_key_id = SerializableSecretString::from(decrypt_text(
                        encrypted_s3_access_key_id,
                        encryption_key,
                    )?);

                    let s3_secret_access_key = SerializableSecretString::from(decrypt_text(
                        encrypted_s3_secret_access_key,
                        encryption_key,
                    )?);

                    Ok(StoredDestinationConfig::Iceberg {
                        config: StoredIcebergConfig::Rest {
                            catalog_uri,
                            warehouse_name,
                            namespace,
                            s3_access_key_id,
                            s3_secret_access_key,
                            s3_endpoint,
                        },
                    })
                }
            },
            EncryptedStoredDestinationConfig::ClickHouse { url, user, password, database } => {
                let password = match password {
                    Some(p) => {
                        Some(SerializableSecretString::from(decrypt_text(p, encryption_key)?))
                    }
                    None => None,
                };

                Ok(StoredDestinationConfig::ClickHouse { url, user, password, database })
            }
            Self::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                duckdb_memory_cache_limit,
                maintenance_target_file_size,
            } => Ok(StoredDestinationConfig::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id: s3_access_key_id
                    .map(|value| {
                        decrypt_text(value, encryption_key).map(SerializableSecretString::from)
                    })
                    .transpose()?,
                s3_secret_access_key: s3_secret_access_key
                    .map(|value| {
                        decrypt_text(value, encryption_key).map(SerializableSecretString::from)
                    })
                    .transpose()?,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                duckdb_memory_cache_limit,
                maintenance_target_file_size,
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub enum StoredIcebergConfig {
    Supabase {
        project_ref: String,
        warehouse_name: String,
        namespace: Option<String>,
        catalog_token: SerializableSecretString,
        s3_access_key_id: SerializableSecretString,
        s3_secret_access_key: SerializableSecretString,
        s3_region: String,
    },
    Rest {
        catalog_uri: String,
        warehouse_name: String,
        namespace: Option<String>,
        s3_access_key_id: SerializableSecretString,
        s3_secret_access_key: SerializableSecretString,
        s3_endpoint: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FullApiIcebergConfig {
    Supabase {
        #[schema(example = "abcdefghijklmnopqrst")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        project_ref: String,
        #[schema(example = "my-warehouse")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        warehouse_name: String,
        #[schema(example = "my-namespace")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        namespace: Option<String>,
        #[schema(
            example = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6IjFkNzFjMGEyNmIxMDFjODQ5ZTkxZmQ1NjdjYjA5NTJmIn0.eyJleHAiOjIwNzA3MTcxNjAsImlhdCI6MTc1NjE0NTE1MCwiaXNzIjoic3VwYWJhc2UiLCJyZWYiOiJhYmNkZWZnaGlqbGttbm9wcXJzdCIsInJvbGUiOiJzZXJ2aWNlX3JvbGUifQ.YdTWkkIvwjSkXot3NC07xyjPjGWQMNzLq5EPzumzrdLzuHrj-zuzI-nlyQtQ5V7gZauysm-wGwmpztRXfPc3AQ"
        )]
        catalog_token: SerializableSecretString,
        #[schema(example = "9156667efc2c70d89af6588da86d2924")]
        s3_access_key_id: SerializableSecretString,
        #[schema(example = "ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4")]
        s3_secret_access_key: SerializableSecretString,
        #[schema(example = "ap-southeast-1")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        s3_region: String,
    },
    Rest {
        #[schema(example = "https://abcdefghijklmnopqrst.storage.supabase.com/storage/v1/iceberg")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        catalog_uri: String,
        #[schema(example = "my-warehouse")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        warehouse_name: String,
        #[schema(example = "my-namespace")]
        #[serde(default, deserialize_with = "crate::utils::trim_option_string")]
        namespace: Option<String>,
        #[schema(example = "9156667efc2c70d89af6588da86d2924")]
        s3_access_key_id: SerializableSecretString,
        #[schema(example = "ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4")]
        s3_secret_access_key: SerializableSecretString,
        #[schema(example = "https://s3.endpoint")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        s3_endpoint: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EncryptedStoredIcebergConfig {
    Supabase {
        project_ref: String,
        warehouse_name: String,
        namespace: Option<String>,
        catalog_token: EncryptedValue,
        s3_access_key_id: EncryptedValue,
        s3_secret_access_key: EncryptedValue,
        s3_region: String,
    },
    Rest {
        catalog_uri: String,
        warehouse_name: String,
        namespace: Option<String>,
        s3_access_key_id: EncryptedValue,
        s3_secret_access_key: EncryptedValue,
        s3_endpoint: String,
    },
}

#[cfg(test)]
mod tests {
    use insta::assert_json_snapshot;

    use super::*;
    use crate::configs::encryption::{EncryptionKey, generate_random_key};

    #[test]
    fn stored_destination_config_encryption_decryption_bigquery() {
        let config = StoredDestinationConfig::BigQuery {
            project_id: "test-project".to_string(),
            dataset_id: "test_dataset".to_string(),
            service_account_key: SerializableSecretString::from("{\"test\": \"key\"}".to_string()),
            max_staleness_mins: Some(15),
            connection_pool_size: 8,
        };

        let key = EncryptionKey { id: 1, key: generate_random_key::<32>().unwrap() };

        let encrypted = config.clone().encrypt(&key).unwrap();
        let decrypted = encrypted.decrypt(&key).unwrap();

        match (config, decrypted) {
            (
                StoredDestinationConfig::BigQuery {
                    project_id: p1,
                    dataset_id: d1,
                    service_account_key: key1,
                    max_staleness_mins: staleness1,
                    connection_pool_size: connection_pool_size1,
                },
                StoredDestinationConfig::BigQuery {
                    project_id: p2,
                    dataset_id: d2,
                    service_account_key: key2,
                    max_staleness_mins: staleness2,
                    connection_pool_size: connection_pool_size2,
                },
            ) => {
                assert_eq!(p1, p2);
                assert_eq!(d1, d2);
                assert_eq!(staleness1, staleness2);
                assert_eq!(connection_pool_size1, connection_pool_size2);
                // Assert that service account key was encrypted and decrypted correctly
                assert_eq!(key1.expose_secret(), key2.expose_secret());
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn stored_destination_config_encryption_decryption_iceberg_supabase() {
        let config = StoredDestinationConfig::Iceberg {
            config: StoredIcebergConfig::Supabase {
                project_ref: "abcdefghijklmnopqrst".to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: Some("my-namespace".to_string()),
                catalog_token: SerializableSecretString::from("eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6IjFkNzFjMGEyNmIxMDFjODQ5ZTkxZmQ1NjdjYjA5NTJmIn0.eyJleHAiOjIwNzA3MTcxNjAsImlhdCI6MTc1NjE0NTE1MCwiaXNzIjoic3VwYWJhc2UiLCJyZWYiOiJhYmNkZWZnaGlqbGttbm9wcXJzdCIsInJvbGUiOiJzZXJ2aWNlX3JvbGUifQ.YdTWkkIvwjSkXot3NC07xyjPjGWQMNzLq5EPzumzrdLzuHrj-zuzI-nlyQtQ5V7gZauysm-wGwmpztRXfPc3AQ".to_string()),
                s3_access_key_id: SerializableSecretString::from("9156667efc2c70d89af6588da86d2924".to_string()),
                s3_secret_access_key: SerializableSecretString::from("ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4".to_string()),
                s3_region: "ap-southeast-1".to_string(),
            },
        };

        let key = EncryptionKey { id: 1, key: generate_random_key::<32>().unwrap() };

        let encrypted = config.clone().encrypt(&key).unwrap();
        let decrypted = encrypted.decrypt(&key).unwrap();

        match (config, decrypted) {
            (
                StoredDestinationConfig::Iceberg {
                    config:
                        StoredIcebergConfig::Supabase {
                            project_ref: p1_project_ref,
                            warehouse_name: p1_warehouse_name,
                            namespace: p1_namespace,
                            catalog_token: p1_catalog_token,
                            s3_access_key_id: p1_s3_access_key_id,
                            s3_secret_access_key: p1_s3_secret_access_key,
                            s3_region: p1_s3_region,
                        },
                },
                StoredDestinationConfig::Iceberg {
                    config:
                        StoredIcebergConfig::Supabase {
                            project_ref: p2_project_ref,
                            warehouse_name: p2_warehouse_name,
                            namespace: p2_namespace,
                            catalog_token: p2_catalog_token,
                            s3_access_key_id: p2_s3_access_key_id,
                            s3_secret_access_key: p2_s3_secret_access_key,
                            s3_region: p2_s3_region,
                        },
                },
            ) => {
                assert_eq!(p1_project_ref, p2_project_ref);
                assert_eq!(p1_warehouse_name, p2_warehouse_name);
                assert_eq!(p1_namespace, p2_namespace);
                assert_eq!(
                    p1_s3_access_key_id.expose_secret(),
                    p2_s3_access_key_id.expose_secret()
                );
                assert_eq!(p1_s3_region, p2_s3_region);
                // Assert that secret fields were encrypted and decrypted correctly
                assert_eq!(p1_catalog_token.expose_secret(), p2_catalog_token.expose_secret());
                assert_eq!(
                    p1_s3_secret_access_key.expose_secret(),
                    p2_s3_secret_access_key.expose_secret()
                );
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn stored_destination_config_encryption_decryption_iceberg_rest() {
        let config = StoredDestinationConfig::Iceberg {
            config: StoredIcebergConfig::Rest {
                catalog_uri: "https://abcdefghijklmnopqrst.storage.supabase.com/storage/v1/iceberg"
                    .to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: Some("my-namespace".to_string()),
                s3_access_key_id: SerializableSecretString::from("id".to_string()),
                s3_secret_access_key: SerializableSecretString::from("key".to_string()),
                s3_endpoint: "http://localhost:8080".to_string(),
            },
        };

        let key = EncryptionKey { id: 1, key: generate_random_key::<32>().unwrap() };

        let encrypted = config.clone().encrypt(&key).unwrap();
        let decrypted = encrypted.decrypt(&key).unwrap();

        match (config, decrypted) {
            (
                StoredDestinationConfig::Iceberg {
                    config:
                        StoredIcebergConfig::Rest {
                            catalog_uri: p1_catalog_uri,
                            warehouse_name: p1_warehouse_name,
                            namespace: p1_namespace,
                            s3_access_key_id: p1_s3_access_key_id,
                            s3_secret_access_key: p1_s3_secret_access_key,
                            s3_endpoint: p1_s3_endpoint,
                        },
                },
                StoredDestinationConfig::Iceberg {
                    config:
                        StoredIcebergConfig::Rest {
                            catalog_uri: p2_catalog_uri,
                            warehouse_name: p2_warehouse_name,
                            namespace: p2_namespace,
                            s3_access_key_id: p2_s3_access_key_id,
                            s3_secret_access_key: p2_s3_secret_access_key,
                            s3_endpoint: p2_s3_endpoint,
                        },
                },
            ) => {
                assert_eq!(p1_catalog_uri, p2_catalog_uri);
                assert_eq!(p1_warehouse_name, p2_warehouse_name);
                assert_eq!(p1_namespace, p2_namespace);
                assert_eq!(
                    p1_s3_access_key_id.expose_secret(),
                    p2_s3_access_key_id.expose_secret()
                );
                assert_eq!(
                    p1_s3_secret_access_key.expose_secret(),
                    p2_s3_secret_access_key.expose_secret()
                );
                assert_eq!(p1_s3_endpoint, p2_s3_endpoint);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn stored_destination_config_encryption_decryption_clickhouse() {
        let config = StoredDestinationConfig::ClickHouse {
            url: Url::parse("https://example.com:8443").unwrap(),
            user: "etl".to_string(),
            password: Some(SerializableSecretString::from("secret".to_string())),
            database: "analytics".to_string(),
        };

        let key = EncryptionKey { id: 1, key: generate_random_key::<32>().unwrap() };

        let encrypted = config.clone().encrypt(&key).unwrap();
        let decrypted = encrypted.decrypt(&key).unwrap();

        match (config, decrypted) {
            (
                StoredDestinationConfig::ClickHouse {
                    url: u1,
                    user: user1,
                    password: p1,
                    database: d1,
                },
                StoredDestinationConfig::ClickHouse {
                    url: u2,
                    user: user2,
                    password: p2,
                    database: d2,
                },
            ) => {
                assert_eq!(u1, u2);
                assert_eq!(user1, user2);
                assert_eq!(d1, d2);
                assert_eq!(
                    p1.as_ref().map(|value| value.expose_secret()),
                    p2.as_ref().map(|value| value.expose_secret())
                );
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn full_api_destination_config_conversion_clickhouse() {
        let full_config = FullApiDestinationConfig::ClickHouse {
            url: Url::parse("https://example.com:8443").unwrap(),
            user: "etl".to_string(),
            password: Some(SerializableSecretString::from("secret".to_string())),
            database: "analytics".to_string(),
        };

        let stored: StoredDestinationConfig = full_config.clone().into();
        let back_to_full: FullApiDestinationConfig = stored.into();

        match (full_config, back_to_full) {
            (
                FullApiDestinationConfig::ClickHouse {
                    url: u1,
                    user: user1,
                    password: p1,
                    database: d1,
                },
                FullApiDestinationConfig::ClickHouse {
                    url: u2,
                    user: user2,
                    password: p2,
                    database: d2,
                },
            ) => {
                assert_eq!(u1, u2);
                assert_eq!(user1, user2);
                assert_eq!(d1, d2);
                assert_eq!(
                    p1.as_ref().map(|value| value.expose_secret()),
                    p2.as_ref().map(|value| value.expose_secret())
                );
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn full_api_destination_config_deserializes_clickhouse_url() {
        let json = r#"
        {
            "click_house": {
                "url": "  https://example.com:8443  ",
                "user": "etl",
                "database": "analytics"
            }
        }
        "#;

        let deserialized: FullApiDestinationConfig = serde_json::from_str(json).unwrap();
        match deserialized {
            FullApiDestinationConfig::ClickHouse { url, user, password, database } => {
                assert_eq!(url.as_str(), "https://example.com:8443/");
                assert_eq!(user, "etl");
                assert!(password.is_none());
                assert_eq!(database, "analytics");
            }
            _ => panic!("Deserialization failed or variant mismatch"),
        }
    }

    #[test]
    fn full_api_destination_config_rejects_non_http_clickhouse_url() {
        let json = r#"
        {
            "click_house": {
                "url": "ftp://example.com/data",
                "user": "etl",
                "database": "analytics"
            }
        }
        "#;

        let error = serde_json::from_str::<FullApiDestinationConfig>(json).unwrap_err();
        assert!(error.to_string().contains("url must use http or https scheme"));
    }

    #[test]
    fn full_api_destination_config_conversion_bigquery() {
        let full_config = FullApiDestinationConfig::BigQuery {
            project_id: "test-project".to_string(),
            dataset_id: "test_dataset".to_string(),
            service_account_key: SerializableSecretString::from("{\"test\": \"key\"}".to_string()),
            max_staleness_mins: Some(15),
            connection_pool_size: None,
        };

        let stored: StoredDestinationConfig = full_config.clone().into();
        let back_to_full: FullApiDestinationConfig = stored.into();

        match (full_config, back_to_full) {
            (
                FullApiDestinationConfig::BigQuery {
                    project_id: p1_project_id,
                    dataset_id: p1_dataset_id,
                    service_account_key: p1_service_account_key,
                    max_staleness_mins: p1_max_staleness_mins,
                    connection_pool_size: p1_connection_pool_size,
                },
                FullApiDestinationConfig::BigQuery {
                    project_id: p2_project_id,
                    dataset_id: p2_dataset_id,
                    service_account_key: p2_service_account_key,
                    max_staleness_mins: p2_max_staleness_mins,
                    connection_pool_size: p2_connection_pool_size,
                },
            ) => {
                assert_eq!(p1_project_id, p2_project_id);
                assert_eq!(p1_dataset_id, p2_dataset_id);
                assert_eq!(
                    p1_service_account_key.expose_secret(),
                    p2_service_account_key.expose_secret()
                );
                assert_eq!(p1_max_staleness_mins, p2_max_staleness_mins);
                // Note: connection_pool_size should be set to DEFAULT_POOL_SIZE when None
                assert_eq!(p1_connection_pool_size, None);
                assert_eq!(
                    p2_connection_pool_size,
                    Some(DestinationConfig::DEFAULT_CONNECTION_POOL_SIZE)
                );
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn full_api_destination_config_conversion_iceberg_supabase() {
        let full_config = FullApiDestinationConfig::Iceberg {
            config: FullApiIcebergConfig::Supabase {
                project_ref: "abcdefghijklmnopqrst".to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: Some("my-namespace".to_string()),
                catalog_token: SerializableSecretString::from("eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6IjFkNzFjMGEyNmIxMDFjODQ5ZTkxZmQ1NjdjYjA5NTJmIn0.eyJleHAiOjIwNzA3MTcxNjAsImlhdCI6MTc1NjE0NTE1MCwiaXNzIjoic3VwYWJhc2UiLCJyZWYiOiJhYmNkZWZnaGlqbGttbm9wcXJzdCIsInJvbGUiOiJzZXJ2aWNlX3JvbGUifQ.YdTWkkIvwjSkXot3NC07xyjPjGWQMNzLq5EPzumzrdLzuHrj-zuzI-nlyQtQ5V7gZauysm-wGwmpztRXfPc3AQ".to_string()),
                s3_access_key_id: SerializableSecretString::from("9156667efc2c70d89af6588da86d2924".to_string()),
                s3_secret_access_key: SerializableSecretString::from("ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4".to_string()),
                s3_region: "ap-southeast-1".to_string(),
            },
        };

        let stored: StoredDestinationConfig = full_config.clone().into();
        let back_to_full: FullApiDestinationConfig = stored.into();

        match (full_config, back_to_full) {
            (
                FullApiDestinationConfig::Iceberg {
                    config:
                        FullApiIcebergConfig::Supabase {
                            project_ref: p1_project_ref,
                            warehouse_name: p1_warehouse_name,
                            namespace: p1_namespace,
                            catalog_token: p1_catalog_token,
                            s3_access_key_id: p1_s3_access_key_id,
                            s3_secret_access_key: p1_s3_secret_access_key,
                            s3_region: p1_s3_region,
                        },
                },
                FullApiDestinationConfig::Iceberg {
                    config:
                        FullApiIcebergConfig::Supabase {
                            project_ref: p2_project_ref,
                            warehouse_name: p2_warehouse_name,
                            namespace: p2_namespace,
                            catalog_token: p2_catalog_token,
                            s3_access_key_id: p2_s3_access_key_id,
                            s3_secret_access_key: p2_s3_secret_access_key,
                            s3_region: p2_s3_region,
                        },
                },
            ) => {
                assert_eq!(p1_project_ref, p2_project_ref);
                assert_eq!(p1_warehouse_name, p2_warehouse_name);
                assert_eq!(p1_namespace, p2_namespace);
                assert_eq!(p1_catalog_token.expose_secret(), p2_catalog_token.expose_secret());
                assert_eq!(
                    p1_s3_access_key_id.expose_secret(),
                    p2_s3_access_key_id.expose_secret()
                );
                assert_eq!(
                    p1_s3_secret_access_key.expose_secret(),
                    p2_s3_secret_access_key.expose_secret()
                );
                assert_eq!(p1_s3_region, p2_s3_region);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn full_api_destination_config_conversion_iceberg_rest() {
        let full_config = FullApiDestinationConfig::Iceberg {
            config: FullApiIcebergConfig::Rest {
                catalog_uri: "https://abcdefghijklmnopqrst.storage.supabase.com/storage/v1/iceberg"
                    .to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: Some("my-namespace".to_string()),
                s3_access_key_id: SerializableSecretString::from("id".to_string()),
                s3_secret_access_key: SerializableSecretString::from("key".to_string()),
                s3_endpoint: "http://localhost:8080".to_string(),
            },
        };

        let stored: StoredDestinationConfig = full_config.clone().into();
        let back_to_full: FullApiDestinationConfig = stored.into();

        match (full_config, back_to_full) {
            (
                FullApiDestinationConfig::Iceberg {
                    config:
                        FullApiIcebergConfig::Rest {
                            catalog_uri: p1_catalog_uri,
                            warehouse_name: p1_warehouse_name,
                            namespace: p1_namespace,
                            s3_access_key_id: p1_s3_access_key_id,
                            s3_secret_access_key: p1_s3_secret_access_key,
                            s3_endpoint: p1_s3_endpoint,
                        },
                },
                FullApiDestinationConfig::Iceberg {
                    config:
                        FullApiIcebergConfig::Rest {
                            catalog_uri: p2_catalog_uri,
                            warehouse_name: p2_warehouse_name,
                            namespace: p2_namespace,
                            s3_access_key_id: p2_s3_access_key_id,
                            s3_secret_access_key: p2_s3_secret_access_key,
                            s3_endpoint: p2_s3_endpoint,
                        },
                },
            ) => {
                assert_eq!(p1_catalog_uri, p2_catalog_uri);
                assert_eq!(p1_warehouse_name, p2_warehouse_name);
                assert_eq!(p1_namespace, p2_namespace);
                assert_eq!(
                    p1_s3_access_key_id.expose_secret(),
                    p2_s3_access_key_id.expose_secret()
                );
                assert_eq!(
                    p1_s3_secret_access_key.expose_secret(),
                    p2_s3_secret_access_key.expose_secret()
                );
                assert_eq!(p1_s3_endpoint, p2_s3_endpoint);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn stored_destination_config_encryption_decryption_ducklake() {
        let config = StoredDestinationConfig::Ducklake {
            catalog_url: "postgres://user:pass@localhost:5432/ducklake_catalog".to_string(),
            data_path: "s3://bucket/path".to_string(),
            pool_size: 8,
            s3_access_key_id: Some(SerializableSecretString::from("access".to_string())),
            s3_secret_access_key: Some(SerializableSecretString::from("secret".to_string())),
            s3_region: Some("us-east-1".to_string()),
            s3_endpoint: Some("127.0.0.1:5000/s3".to_string()),
            s3_url_style: Some("path".to_string()),
            s3_use_ssl: Some(false),
            metadata_schema: Some("ducklake".to_string()),
            duckdb_memory_cache_limit: Some("50MB".to_string()),
            maintenance_target_file_size: Some("10MB".to_string()),
        };

        let key = EncryptionKey { id: 1, key: generate_random_key::<32>().unwrap() };

        let encrypted = config.clone().encrypt(&key).unwrap();
        let decrypted = encrypted.decrypt(&key).unwrap();

        match (config, decrypted) {
            (
                StoredDestinationConfig::Ducklake {
                    catalog_url: c1,
                    data_path: d1,
                    pool_size: p1,
                    s3_access_key_id: a1,
                    s3_secret_access_key: s1,
                    s3_region: r1,
                    s3_endpoint: e1,
                    s3_url_style: u1,
                    s3_use_ssl: ssl1,
                    metadata_schema: m1,
                    duckdb_memory_cache_limit: memory1,
                    maintenance_target_file_size: target1,
                },
                StoredDestinationConfig::Ducklake {
                    catalog_url: c2,
                    data_path: d2,
                    pool_size: p2,
                    s3_access_key_id: a2,
                    s3_secret_access_key: s2,
                    s3_region: r2,
                    s3_endpoint: e2,
                    s3_url_style: u2,
                    s3_use_ssl: ssl2,
                    metadata_schema: m2,
                    duckdb_memory_cache_limit: memory2,
                    maintenance_target_file_size: target2,
                },
            ) => {
                assert_eq!(c1, c2);
                assert_eq!(d1, d2);
                assert_eq!(p1, p2);
                assert_eq!(
                    a1.as_ref().map(|value| value.expose_secret()),
                    a2.as_ref().map(|value| value.expose_secret())
                );
                assert_eq!(
                    s1.as_ref().map(|value| value.expose_secret()),
                    s2.as_ref().map(|value| value.expose_secret())
                );
                assert_eq!(r1, r2);
                assert_eq!(e1, e2);
                assert_eq!(u1, u2);
                assert_eq!(ssl1, ssl2);
                assert_eq!(m1, m2);
                assert_eq!(memory1, memory2);
                assert_eq!(target1, target2);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn full_api_destination_config_conversion_ducklake() {
        let full_config = FullApiDestinationConfig::Ducklake {
            catalog_url: "postgres://user:pass@localhost:5432/ducklake_catalog".to_string(),
            data_path: "file:///absolute/path/to/lake_data".to_string(),
            pool_size: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_region: None,
            s3_endpoint: None,
            s3_url_style: None,
            s3_use_ssl: None,
            metadata_schema: Some("ducklake".to_string()),
            duckdb_memory_cache_limit: None,
            maintenance_target_file_size: None,
        };

        let stored: StoredDestinationConfig = full_config.clone().into();
        let back_to_full: FullApiDestinationConfig = stored.into();

        match (full_config, back_to_full) {
            (
                FullApiDestinationConfig::Ducklake {
                    catalog_url: c1,
                    data_path: d1,
                    pool_size: p1,
                    metadata_schema: m1,
                    duckdb_memory_cache_limit: memory1,
                    maintenance_target_file_size: target1,
                    ..
                },
                FullApiDestinationConfig::Ducklake {
                    catalog_url: c2,
                    data_path: d2,
                    pool_size: p2,
                    metadata_schema: m2,
                    duckdb_memory_cache_limit: memory2,
                    maintenance_target_file_size: target2,
                    ..
                },
            ) => {
                assert_eq!(c1, c2);
                assert_eq!(d1, d2);
                assert_eq!(p1, None);
                assert_eq!(p2, Some(DestinationConfig::DEFAULT_DUCKLAKE_POOL_SIZE));
                assert_eq!(m1, m2);
                assert_eq!(memory1, memory2);
                assert_eq!(target1, target2);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn full_api_destination_config_serialization_ducklake() {
        let full_config = FullApiDestinationConfig::Ducklake {
            catalog_url: "postgres://user:pass@localhost:5432/ducklake_catalog".to_string(),
            data_path: "s3://bucket/path".to_string(),
            pool_size: Some(4),
            s3_access_key_id: Some(SerializableSecretString::from("access".to_string())),
            s3_secret_access_key: Some(SerializableSecretString::from("secret".to_string())),
            s3_region: Some("us-east-1".to_string()),
            s3_endpoint: Some("127.0.0.1:5000/s3".to_string()),
            s3_url_style: Some("path".to_string()),
            s3_use_ssl: Some(false),
            metadata_schema: Some("ducklake".to_string()),
            duckdb_memory_cache_limit: Some("50MB".to_string()),
            maintenance_target_file_size: Some("10MB".to_string()),
        };

        assert_json_snapshot!(full_config);

        let json = serde_json::to_string_pretty(&full_config).unwrap();
        let deserialized: FullApiDestinationConfig = serde_json::from_str(&json).unwrap();
        match (&full_config, deserialized) {
            (
                FullApiDestinationConfig::Ducklake {
                    catalog_url: c1,
                    data_path: d1,
                    pool_size: p1,
                    s3_access_key_id: a1,
                    s3_secret_access_key: s1,
                    s3_region: r1,
                    s3_endpoint: e1,
                    s3_url_style: u1,
                    s3_use_ssl: ssl1,
                    metadata_schema: m1,
                    duckdb_memory_cache_limit: memory1,
                    maintenance_target_file_size: target1,
                },
                FullApiDestinationConfig::Ducklake {
                    catalog_url: c2,
                    data_path: d2,
                    pool_size: p2,
                    s3_access_key_id: a2,
                    s3_secret_access_key: s2,
                    s3_region: r2,
                    s3_endpoint: e2,
                    s3_url_style: u2,
                    s3_use_ssl: ssl2,
                    metadata_schema: m2,
                    duckdb_memory_cache_limit: memory2,
                    maintenance_target_file_size: target2,
                },
            ) => {
                assert_eq!(c1, &c2);
                assert_eq!(d1, &d2);
                assert_eq!(p1, &p2);
                assert_eq!(
                    a1.as_ref().map(|value| value.expose_secret()),
                    a2.as_ref().map(|value| value.expose_secret())
                );
                assert_eq!(
                    s1.as_ref().map(|value| value.expose_secret()),
                    s2.as_ref().map(|value| value.expose_secret())
                );
                assert_eq!(r1, &r2);
                assert_eq!(e1, &e2);
                assert_eq!(u1, &u2);
                assert_eq!(ssl1, &ssl2);
                assert_eq!(m1, &m2);
                assert_eq!(memory1, &memory2);
                assert_eq!(target1, &target2);
            }
            _ => panic!("Deserialization failed or variant mismatch"),
        }
    }

    #[test]
    fn full_api_destination_config_serialization_iceberg_supabase() {
        let full_config = FullApiDestinationConfig::Iceberg {
            config: FullApiIcebergConfig::Supabase {
                project_ref: "abcdefghijklmnopqrst".to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: Some("my-namespace".to_string()),
                catalog_token: SerializableSecretString::from("token123".to_string()),
                s3_access_key_id: SerializableSecretString::from("access_key_123".to_string()),
                s3_secret_access_key: SerializableSecretString::from("secret123".to_string()),
                s3_region: "us-west-2".to_string(),
            },
        };

        // Use snapshot testing to verify the exact JSON structure
        assert_json_snapshot!(full_config);

        // Test that we can deserialize it back and all fields match
        let json = serde_json::to_string_pretty(&full_config).unwrap();
        let deserialized: FullApiDestinationConfig = serde_json::from_str(&json).unwrap();
        match (&full_config, deserialized) {
            (
                FullApiDestinationConfig::Iceberg {
                    config:
                        FullApiIcebergConfig::Supabase {
                            project_ref: orig_project_ref,
                            warehouse_name: orig_warehouse_name,
                            namespace: orig_namespace,
                            catalog_token: orig_catalog_token,
                            s3_access_key_id: orig_s3_access_key_id,
                            s3_secret_access_key: orig_s3_secret_access_key,
                            s3_region: orig_s3_region,
                        },
                },
                FullApiDestinationConfig::Iceberg {
                    config:
                        FullApiIcebergConfig::Supabase {
                            project_ref: deser_project_ref,
                            warehouse_name: deser_warehouse_name,
                            namespace: deser_namespace,
                            catalog_token: deser_catalog_token,
                            s3_access_key_id: deser_s3_access_key_id,
                            s3_secret_access_key: deser_s3_secret_access_key,
                            s3_region: deser_s3_region,
                        },
                },
            ) => {
                assert_eq!(orig_project_ref, &deser_project_ref);
                assert_eq!(orig_warehouse_name, &deser_warehouse_name);
                assert_eq!(orig_namespace, &deser_namespace);
                assert_eq!(orig_catalog_token.expose_secret(), deser_catalog_token.expose_secret());
                assert_eq!(
                    orig_s3_access_key_id.expose_secret(),
                    deser_s3_access_key_id.expose_secret()
                );
                assert_eq!(
                    orig_s3_secret_access_key.expose_secret(),
                    deser_s3_secret_access_key.expose_secret()
                );
                assert_eq!(orig_s3_region, &deser_s3_region);
            }
            _ => panic!("Deserialization failed or variant mismatch"),
        }
    }

    #[test]
    fn full_api_destination_config_serialization_iceberg_rest() {
        let full_config = FullApiDestinationConfig::Iceberg {
            config: FullApiIcebergConfig::Rest {
                catalog_uri: "https://catalog.example.com/iceberg".to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: Some("my-namespace".to_string()),
                s3_access_key_id: SerializableSecretString::from("id".to_string()),
                s3_secret_access_key: SerializableSecretString::from("key".to_string()),
                s3_endpoint: "http://localhost:8080".to_string(),
            },
        };

        // Use snapshot testing to verify the exact JSON structure
        assert_json_snapshot!(full_config);

        // Test that we can deserialize it back and all fields match
        let json = serde_json::to_string_pretty(&full_config).unwrap();
        let deserialized: FullApiDestinationConfig = serde_json::from_str(&json).unwrap();
        match (&full_config, deserialized) {
            (
                FullApiDestinationConfig::Iceberg {
                    config:
                        FullApiIcebergConfig::Rest {
                            catalog_uri: orig_catalog_uri,
                            warehouse_name: orig_warehouse_name,
                            namespace: orig_namespace,
                            s3_access_key_id: p1_s3_access_key_id,
                            s3_secret_access_key: p1_s3_secret_access_key,
                            s3_endpoint: p1_s3_endpoint,
                        },
                },
                FullApiDestinationConfig::Iceberg {
                    config:
                        FullApiIcebergConfig::Rest {
                            catalog_uri: deser_catalog_uri,
                            warehouse_name: deser_warehouse_name,
                            namespace: deser_namespace,
                            s3_access_key_id: p2_s3_access_key_id,
                            s3_secret_access_key: p2_s3_secret_access_key,
                            s3_endpoint: p2_s3_endpoint,
                        },
                },
            ) => {
                assert_eq!(orig_catalog_uri, &deser_catalog_uri);
                assert_eq!(orig_warehouse_name, &deser_warehouse_name);
                assert_eq!(orig_namespace, &deser_namespace);
                assert_eq!(
                    p1_s3_access_key_id.expose_secret(),
                    p2_s3_access_key_id.expose_secret()
                );
                assert_eq!(
                    p1_s3_secret_access_key.expose_secret(),
                    p2_s3_secret_access_key.expose_secret()
                );
                assert_eq!(p1_s3_endpoint, &p2_s3_endpoint);
            }
            _ => panic!("Deserialization failed or variant mismatch"),
        }
    }
}
