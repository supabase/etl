use etl_config::SerializableSecretString;
use etl_config::shared::DestinationConfig;
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
        max_staleness_mins: Option<u16>,
        #[schema(example = 8)]
        max_concurrent_streams: Option<usize>,
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
                service_account_key,
                max_staleness_mins,
                max_concurrent_streams,
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::encryption::{EncryptionKey, generate_random_key};

    #[test]
    fn test_stored_destination_config_serialization() {
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
                StoredDestinationConfig::BigQuery { project_id: p1, .. },
                StoredDestinationConfig::BigQuery { project_id: p2, .. },
            ) => {
                assert_eq!(p1, p2);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn test_stored_destination_config_encryption_decryption() {
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
    fn test_full_api_destination_config_conversion() {
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
                FullApiDestinationConfig::BigQuery { project_id: p1, .. },
                FullApiDestinationConfig::BigQuery { project_id: p2, .. },
            ) => {
                assert_eq!(p1, p2);
            }
            _ => panic!("Config types don't match"),
        }
    }
}
