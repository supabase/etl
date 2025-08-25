use crate::configs::encryption::{
    Decrypt, DecryptionError, Encrypt, EncryptedValue, EncryptionError, EncryptionKey,
    decrypt_text, encrypt_text,
};
use etl_config::SerializableSecretString;
use etl_config::shared::DestinationConfig;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

const DEFAULT_MAX_CONCURRENT_STREAMS: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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

#[derive(Debug, Clone, Serialize)]
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
