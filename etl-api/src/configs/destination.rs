use etl_config::SerializableSecretString;
use etl_config::shared::{DestinationConfig, IcebergConfig};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::configs::encryption::{
    Decrypt, DecryptionError, Encrypt, EncryptedValue, EncryptionError, EncryptionKey,
    decrypt_text, encrypt_text,
};
use crate::configs::store::Store;

const DEFAULT_MAX_CONCURRENT_STREAMS: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FullApiDestinationConfig {
    Memory,
    BigQuery {
        #[schema(example = "my-gcp-project")]
        project_id: String,
        #[schema(example = "my_dataset")]
        dataset_id: String,
        #[schema(example = "{\"type\": \"service_account\", \"project_id\": \"my-project\"}")]
        service_account_key: SerializableSecretString,
        #[schema(example = 15)]
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
        #[schema(example = 8)]
        #[serde(skip_serializing_if = "Option::is_none")]
        max_concurrent_streams: Option<usize>,
    },
    Iceberg {
        #[serde(flatten)]
        config: FullApiIcebergConfig,
    },
}

impl From<StoredDestinationConfig> for FullApiDestinationConfig {
    fn from(value: StoredDestinationConfig) -> Self {
        match value {
            StoredDestinationConfig::Memory => Self::Memory,
            StoredDestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                max_concurrent_streams,
            } => Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                max_concurrent_streams: Some(max_concurrent_streams),
            },
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
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredDestinationConfig {
    Memory,
    BigQuery {
        project_id: String,
        dataset_id: String,
        service_account_key: SerializableSecretString,
        max_staleness_mins: Option<u16>,
        max_concurrent_streams: usize,
    },
    Iceberg {
        #[serde(flatten)]
        config: StoredIcebergConfig,
    },
}

impl StoredDestinationConfig {
    pub fn into_etl_config(self) -> DestinationConfig {
        match self {
            Self::Memory => DestinationConfig::Memory,
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                max_concurrent_streams,
            } => DestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key: service_account_key.into(),
                max_staleness_mins,
                max_concurrent_streams,
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
        }
    }
}

impl From<FullApiDestinationConfig> for StoredDestinationConfig {
    fn from(value: FullApiDestinationConfig) -> Self {
        match value {
            FullApiDestinationConfig::Memory => Self::Memory,
            FullApiDestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                max_concurrent_streams,
            } => Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                max_concurrent_streams: max_concurrent_streams
                    .unwrap_or(DEFAULT_MAX_CONCURRENT_STREAMS),
            },
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
        }
    }
}

impl Encrypt<EncryptedStoredDestinationConfig> for StoredDestinationConfig {
    fn encrypt(
        self,
        encryption_key: &EncryptionKey,
    ) -> Result<EncryptedStoredDestinationConfig, EncryptionError> {
        match self {
            Self::Memory => Ok(EncryptedStoredDestinationConfig::Memory),
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                max_concurrent_streams,
            } => {
                let encrypted_service_account_key = encrypt_text(
                    service_account_key.expose_secret().to_owned(),
                    encryption_key,
                )?;

                Ok(EncryptedStoredDestinationConfig::BigQuery {
                    project_id,
                    dataset_id,
                    service_account_key: encrypted_service_account_key,
                    max_staleness_mins,
                    max_concurrent_streams,
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
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EncryptedStoredDestinationConfig {
    Memory,
    BigQuery {
        project_id: String,
        dataset_id: String,
        service_account_key: EncryptedValue,
        max_staleness_mins: Option<u16>,
        max_concurrent_streams: usize,
    },
    Iceberg {
        #[serde(flatten)]
        config: EncryptedStoredIcebergConfig,
    },
}

impl Store for EncryptedStoredDestinationConfig {}

impl Decrypt<StoredDestinationConfig> for EncryptedStoredDestinationConfig {
    fn decrypt(
        self,
        encryption_key: &EncryptionKey,
    ) -> Result<StoredDestinationConfig, DecryptionError> {
        match self {
            Self::Memory => Ok(StoredDestinationConfig::Memory),
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key: encrypted_service_account_key,
                max_staleness_mins,
                max_concurrent_streams,
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
                    max_concurrent_streams,
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
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredIcebergConfig {
    Supabase {
        project_ref: String,
        warehouse_name: String,
        namespace: String,
        catalog_token: SerializableSecretString,
        s3_access_key_id: SerializableSecretString,
        s3_secret_access_key: SerializableSecretString,
        s3_region: String,
    },
    Rest {
        catalog_uri: String,
        warehouse_name: String,
        namespace: String,
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
        project_ref: String,
        #[schema(example = "my-warehouse")]
        warehouse_name: String,
        #[schema(example = "my-namespace")]
        namespace: String,
        #[schema(
            example = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6IjFkNzFjMGEyNmIxMDFjODQ5ZTkxZmQ1NjdjYjA5NTJmIn0.eyJleHAiOjIwNzA3MTcxNjAsImlhdCI6MTc1NjE0NTE1MCwiaXNzIjoic3VwYWJhc2UiLCJyZWYiOiJhYmNkZWZnaGlqbGttbm9wcXJzdCIsInJvbGUiOiJzZXJ2aWNlX3JvbGUifQ.YdTWkkIvwjSkXot3NC07xyjPjGWQMNzLq5EPzumzrdLzuHrj-zuzI-nlyQtQ5V7gZauysm-wGwmpztRXfPc3AQ"
        )]
        catalog_token: SerializableSecretString,
        #[schema(example = "9156667efc2c70d89af6588da86d2924")]
        s3_access_key_id: SerializableSecretString,
        #[schema(example = "ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4")]
        s3_secret_access_key: SerializableSecretString,
        #[schema(example = "ap-southeast-1")]
        s3_region: String,
    },
    Rest {
        #[schema(example = "https://abcdefghijklmnopqrst.storage.supabase.com/storage/v1/iceberg")]
        catalog_uri: String,
        #[schema(example = "my-warehouse")]
        warehouse_name: String,
        #[schema(example = "my-namespace")]
        namespace: String,
        #[schema(example = "9156667efc2c70d89af6588da86d2924")]
        s3_access_key_id: SerializableSecretString,
        #[schema(example = "ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4")]
        s3_secret_access_key: SerializableSecretString,
        #[schema(example = "https://s3.endpoint")]
        s3_endpoint: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EncryptedStoredIcebergConfig {
    Supabase {
        project_ref: String,
        warehouse_name: String,
        namespace: String,
        catalog_token: EncryptedValue,
        s3_access_key_id: EncryptedValue,
        s3_secret_access_key: EncryptedValue,
        s3_region: String,
    },
    Rest {
        catalog_uri: String,
        warehouse_name: String,
        namespace: String,
        s3_access_key_id: EncryptedValue,
        s3_secret_access_key: EncryptedValue,
        s3_endpoint: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::encryption::{EncryptionKey, generate_random_key};
    use insta::assert_json_snapshot;

    #[test]
    fn test_stored_destination_config_serialization_bigquery() {
        let config = StoredDestinationConfig::BigQuery {
            project_id: "test-project".to_string(),
            dataset_id: "test_dataset".to_string(),
            service_account_key: SerializableSecretString::from("{\"test\": \"key\"}".to_string()),
            max_staleness_mins: Some(15),
            max_concurrent_streams: 8,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: StoredDestinationConfig = serde_json::from_str(&json).unwrap();

        match (config, deserialized) {
            (
                StoredDestinationConfig::BigQuery {
                    project_id: p1_project_id,
                    dataset_id: p1_dataset_id,
                    service_account_key: p1_service_account_key,
                    max_staleness_mins: p1_max_staleness_mins,
                    max_concurrent_streams: p1_max_concurrent_streams,
                },
                StoredDestinationConfig::BigQuery {
                    project_id: p2_project_id,
                    dataset_id: p2_dataset_id,
                    service_account_key: p2_service_account_key,
                    max_staleness_mins: p2_max_staleness_mins,
                    max_concurrent_streams: p2_max_concurrent_streams,
                },
            ) => {
                assert_eq!(p1_project_id, p2_project_id);
                assert_eq!(p1_dataset_id, p2_dataset_id);
                assert_eq!(
                    p1_service_account_key.expose_secret(),
                    p2_service_account_key.expose_secret()
                );
                assert_eq!(p1_max_staleness_mins, p2_max_staleness_mins);
                assert_eq!(p1_max_concurrent_streams, p2_max_concurrent_streams);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn test_stored_destination_config_serialization_iceberg_supabase() {
        let config = StoredDestinationConfig::Iceberg {
            config: StoredIcebergConfig::Supabase {
                project_ref: "abcdefghijklmnopqrst".to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: "my-namespace".to_string(),
                catalog_token: SerializableSecretString::from("eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6IjFkNzFjMGEyNmIxMDFjODQ5ZTkxZmQ1NjdjYjA5NTJmIn0.eyJleHAiOjIwNzA3MTcxNjAsImlhdCI6MTc1NjE0NTE1MCwiaXNzIjoic3VwYWJhc2UiLCJyZWYiOiJhYmNkZWZnaGlqbGttbm9wcXJzdCIsInJvbGUiOiJzZXJ2aWNlX3JvbGUifQ.YdTWkkIvwjSkXot3NC07xyjPjGWQMNzLq5EPzumzrdLzuHrj-zuzI-nlyQtQ5V7gZauysm-wGwmpztRXfPc3AQ".to_string()),
                s3_access_key_id: SerializableSecretString::from("9156667efc2c70d89af6588da86d2924".to_string()),
                s3_secret_access_key: SerializableSecretString::from("ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4".to_string()),
                s3_region: "ap-southeast-1".to_string(),
            },
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: StoredDestinationConfig = serde_json::from_str(&json).unwrap();

        match (deserialized, config) {
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
                    p1_catalog_token.expose_secret(),
                    p2_catalog_token.expose_secret()
                );
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
    fn test_stored_destination_config_serialization_iceberg_rest() {
        let config = StoredDestinationConfig::Iceberg {
            config: StoredIcebergConfig::Rest {
                catalog_uri: "https://abcdefghijklmnopqrst.storage.supabase.com/storage/v1/iceberg"
                    .to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: "my-namespace".to_string(),
                s3_access_key_id: SerializableSecretString::from("id".to_string()),
                s3_secret_access_key: SerializableSecretString::from("key".to_string()),
                s3_endpoint: "http://localhost:8080".to_string(),
            },
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: StoredDestinationConfig = serde_json::from_str(&json).unwrap();

        match (deserialized, config) {
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
    fn test_stored_destination_config_encryption_decryption_bigquery() {
        let config = StoredDestinationConfig::BigQuery {
            project_id: "test-project".to_string(),
            dataset_id: "test_dataset".to_string(),
            service_account_key: SerializableSecretString::from("{\"test\": \"key\"}".to_string()),
            max_staleness_mins: Some(15),
            max_concurrent_streams: 8,
        };

        let key = EncryptionKey {
            id: 1,
            key: generate_random_key::<32>().unwrap(),
        };

        let encrypted = config.clone().encrypt(&key).unwrap();
        let decrypted = encrypted.decrypt(&key).unwrap();

        match (config, decrypted) {
            (
                StoredDestinationConfig::BigQuery {
                    project_id: p1,
                    dataset_id: d1,
                    service_account_key: key1,
                    max_staleness_mins: staleness1,
                    max_concurrent_streams: streams1,
                },
                StoredDestinationConfig::BigQuery {
                    project_id: p2,
                    dataset_id: d2,
                    service_account_key: key2,
                    max_staleness_mins: staleness2,
                    max_concurrent_streams: streams2,
                },
            ) => {
                assert_eq!(p1, p2);
                assert_eq!(d1, d2);
                assert_eq!(staleness1, staleness2);
                assert_eq!(streams1, streams2);
                // Assert that service account key was encrypted and decrypted correctly
                assert_eq!(key1.expose_secret(), key2.expose_secret());
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn test_stored_destination_config_encryption_decryption_iceberg_supabase() {
        let config = StoredDestinationConfig::Iceberg {
            config: StoredIcebergConfig::Supabase {
                project_ref: "abcdefghijklmnopqrst".to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: "my-namespace".to_string(),
                catalog_token: SerializableSecretString::from("eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6IjFkNzFjMGEyNmIxMDFjODQ5ZTkxZmQ1NjdjYjA5NTJmIn0.eyJleHAiOjIwNzA3MTcxNjAsImlhdCI6MTc1NjE0NTE1MCwiaXNzIjoic3VwYWJhc2UiLCJyZWYiOiJhYmNkZWZnaGlqbGttbm9wcXJzdCIsInJvbGUiOiJzZXJ2aWNlX3JvbGUifQ.YdTWkkIvwjSkXot3NC07xyjPjGWQMNzLq5EPzumzrdLzuHrj-zuzI-nlyQtQ5V7gZauysm-wGwmpztRXfPc3AQ".to_string()),
                s3_access_key_id: SerializableSecretString::from("9156667efc2c70d89af6588da86d2924".to_string()),
                s3_secret_access_key: SerializableSecretString::from("ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4".to_string()),
                s3_region: "ap-southeast-1".to_string(),
            },
        };

        let key = EncryptionKey {
            id: 1,
            key: generate_random_key::<32>().unwrap(),
        };

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
                assert_eq!(
                    p1_catalog_token.expose_secret(),
                    p2_catalog_token.expose_secret()
                );
                assert_eq!(
                    p1_s3_secret_access_key.expose_secret(),
                    p2_s3_secret_access_key.expose_secret()
                );
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn test_stored_destination_config_encryption_decryption_iceberg_rest() {
        let config = StoredDestinationConfig::Iceberg {
            config: StoredIcebergConfig::Rest {
                catalog_uri: "https://abcdefghijklmnopqrst.storage.supabase.com/storage/v1/iceberg"
                    .to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: "my-namespace".to_string(),
                s3_access_key_id: SerializableSecretString::from("id".to_string()),
                s3_secret_access_key: SerializableSecretString::from("key".to_string()),
                s3_endpoint: "http://localhost:8080".to_string(),
            },
        };

        let key = EncryptionKey {
            id: 1,
            key: generate_random_key::<32>().unwrap(),
        };

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
    fn test_full_api_destination_config_conversion_bigquery() {
        let full_config = FullApiDestinationConfig::BigQuery {
            project_id: "test-project".to_string(),
            dataset_id: "test_dataset".to_string(),
            service_account_key: SerializableSecretString::from("{\"test\": \"key\"}".to_string()),
            max_staleness_mins: Some(15),
            max_concurrent_streams: None,
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
                    max_concurrent_streams: p1_max_concurrent_streams,
                },
                FullApiDestinationConfig::BigQuery {
                    project_id: p2_project_id,
                    dataset_id: p2_dataset_id,
                    service_account_key: p2_service_account_key,
                    max_staleness_mins: p2_max_staleness_mins,
                    max_concurrent_streams: p2_max_concurrent_streams,
                },
            ) => {
                assert_eq!(p1_project_id, p2_project_id);
                assert_eq!(p1_dataset_id, p2_dataset_id);
                assert_eq!(
                    p1_service_account_key.expose_secret(),
                    p2_service_account_key.expose_secret()
                );
                assert_eq!(p1_max_staleness_mins, p2_max_staleness_mins);
                // Note: max_concurrent_streams should be set to DEFAULT_MAX_CONCURRENT_STREAMS when None
                assert_eq!(p1_max_concurrent_streams, None);
                assert_eq!(
                    p2_max_concurrent_streams,
                    Some(DEFAULT_MAX_CONCURRENT_STREAMS)
                );
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn test_full_api_destination_config_conversion_iceberg_supabase() {
        let full_config = FullApiDestinationConfig::Iceberg {
            config: FullApiIcebergConfig::Supabase {
                project_ref: "abcdefghijklmnopqrst".to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: "my-namespace".to_string(),
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
                assert_eq!(
                    p1_catalog_token.expose_secret(),
                    p2_catalog_token.expose_secret()
                );
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
    fn test_full_api_destination_config_conversion_iceberg_rest() {
        let full_config = FullApiDestinationConfig::Iceberg {
            config: FullApiIcebergConfig::Rest {
                catalog_uri: "https://abcdefghijklmnopqrst.storage.supabase.com/storage/v1/iceberg"
                    .to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: "my-namespace".to_string(),
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
    fn test_full_api_destination_config_serialization_iceberg_supabase() {
        let full_config = FullApiDestinationConfig::Iceberg {
            config: FullApiIcebergConfig::Supabase {
                project_ref: "abcdefghijklmnopqrst".to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: "my-namespace".to_string(),
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
                assert_eq!(
                    orig_catalog_token.expose_secret(),
                    deser_catalog_token.expose_secret()
                );
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
    fn test_full_api_destination_config_serialization_iceberg_rest() {
        let full_config = FullApiDestinationConfig::Iceberg {
            config: FullApiIcebergConfig::Rest {
                catalog_uri: "https://catalog.example.com/iceberg".to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: "my-namespace".to_string(),
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
