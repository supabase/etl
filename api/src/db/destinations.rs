use config::shared::DestinationConfig;
use config::SerializableSecretString;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Transaction};
use std::fmt::Debug;
use thiserror::Error;

use crate::db::serde::{
    decrypt_and_deserialize_from_value, encrypt_and_serialize, DbDeserializationError,
    DbSerializationError,
};
use crate::encryption::{
    decrypt_text, encrypt_text, Decrypt, DecryptionError, Encrypt, EncryptedValue, EncryptionError,
    EncryptionKey,
};

impl Encrypt<EncryptedDestinationConfig> for DestinationConfig {
    fn encrypt(
        self,
        encryption_key: &EncryptionKey,
    ) -> Result<EncryptedDestinationConfig, EncryptionError> {
        match self {
            Self::Memory => Ok(EncryptedDestinationConfig::Memory),
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
            } => {
                let encrypted_service_account_key = encrypt_text(
                    service_account_key.expose_secret().to_owned(),
                    encryption_key,
                )?;

                Ok(EncryptedDestinationConfig::BigQuery {
                    project_id,
                    dataset_id,
                    service_account_key: encrypted_service_account_key,
                    max_staleness_mins,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EncryptedDestinationConfig {
    Memory,
    BigQuery {
        project_id: String,
        dataset_id: String,
        service_account_key: EncryptedValue,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
    },
}

impl Decrypt<DestinationConfig> for EncryptedDestinationConfig {
    fn decrypt(self, encryption_key: &EncryptionKey) -> Result<DestinationConfig, DecryptionError> {
        match self {
            Self::Memory => Ok(DestinationConfig::Memory),
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key: encrypted_service_account_key,
                max_staleness_mins,
            } => {
                let service_account_key = SerializableSecretString::from(decrypt_text(
                    encrypted_service_account_key,
                    encryption_key,
                )?);

                Ok(DestinationConfig::BigQuery {
                    project_id,
                    dataset_id,
                    service_account_key,
                    max_staleness_mins,
                })
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum DestinationsDbError {
    #[error("Error while interacting with PostgreSQL for destinations: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Error while serializing destination config: {0}")]
    DbSerialization(#[from] DbSerializationError),

    #[error("Error while deserializing destination config: {0}")]
    DbDeserialization(#[from] DbDeserializationError),
}

pub struct Destination {
    pub id: i64,
    pub tenant_id: String,
    pub name: String,
    pub config: DestinationConfig,
}

pub async fn create_destination(
    pool: &PgPool,
    tenant_id: &str,
    name: &str,
    config: DestinationConfig,
    encryption_key: &EncryptionKey,
) -> Result<i64, DestinationsDbError> {
    let config = encrypt_and_serialize(config, encryption_key)?;

    let mut txn = pool.begin().await?;
    let res = create_destination_txn(&mut txn, tenant_id, name, config).await;
    txn.commit().await?;

    res
}

pub async fn create_destination_txn(
    txn: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    name: &str,
    config: serde_json::Value,
) -> Result<i64, DestinationsDbError> {
    let record = sqlx::query!(
        r#"
        insert into app.destinations (tenant_id, name, config)
        values ($1, $2, $3)
        returning id
        "#,
        tenant_id,
        name,
        config
    )
    .fetch_one(&mut **txn)
    .await?;

    Ok(record.id)
}

pub async fn read_destination(
    pool: &PgPool,
    tenant_id: &str,
    destination_id: i64,
    encryption_key: &EncryptionKey,
) -> Result<Option<Destination>, DestinationsDbError> {
    let record = sqlx::query!(
        r#"
        select id, tenant_id, name, config
        from app.destinations
        where tenant_id = $1 and id = $2
        "#,
        tenant_id,
        destination_id,
    )
    .fetch_optional(pool)
    .await?;

    let destination = match record {
        Some(record) => {
            let config = decrypt_and_deserialize_from_value::<
                EncryptedDestinationConfig,
                DestinationConfig,
            >(record.config, encryption_key)?;

            let destination = Destination {
                id: record.id,
                tenant_id: record.tenant_id,
                name: record.name,
                config,
            };

            Some(destination)
        }
        None => None,
    };

    Ok(destination)
}

pub async fn update_destination(
    pool: &PgPool,
    tenant_id: &str,
    name: &str,
    destination_id: i64,
    config: DestinationConfig,
    encryption_key: &EncryptionKey,
) -> Result<Option<i64>, DestinationsDbError> {
    let config = encrypt_and_serialize(config, encryption_key)?;

    let mut txn = pool.begin().await?;
    let res = update_destination_txn(&mut txn, tenant_id, name, destination_id, config).await;
    txn.commit().await?;

    res
}

pub async fn update_destination_txn(
    txn: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    name: &str,
    destination_id: i64,
    config: serde_json::Value,
) -> Result<Option<i64>, DestinationsDbError> {
    let record = sqlx::query!(
        r#"
        update app.destinations
        set config = $1, name = $2
        where tenant_id = $3 and id = $4
        returning id
        "#,
        config,
        name,
        tenant_id,
        destination_id
    )
    .fetch_optional(&mut **txn)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_destination(
    pool: &PgPool,
    tenant_id: &str,
    destination_id: i64,
) -> Result<Option<i64>, DestinationsDbError> {
    let record = sqlx::query!(
        r#"
        delete from app.destinations
        where tenant_id = $1 and id = $2
        returning id
        "#,
        tenant_id,
        destination_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_destinations(
    pool: &PgPool,
    tenant_id: &str,
    encryption_key: &EncryptionKey,
) -> Result<Vec<Destination>, DestinationsDbError> {
    let records = sqlx::query!(
        r#"
        select id, tenant_id, name, config
        from app.destinations
        where tenant_id = $1
        "#,
        tenant_id,
    )
    .fetch_all(pool)
    .await?;

    let mut destinations = Vec::with_capacity(records.len());
    for record in records {
        let config = decrypt_and_deserialize_from_value::<
            EncryptedDestinationConfig,
            DestinationConfig,
        >(record.config.clone(), encryption_key)?;

        let destination = Destination {
            id: record.id,
            tenant_id: record.tenant_id,
            name: record.name,
            config,
        };
        destinations.push(destination);
    }

    Ok(destinations)
}

pub async fn destination_exists(
    pool: &PgPool,
    tenant_id: &str,
    destination_id: i64,
) -> Result<bool, DestinationsDbError> {
    let record = sqlx::query!(
        r#"
        select exists (select id
        from app.destinations
        where tenant_id = $1 and id = $2) as "exists!"
        "#,
        tenant_id,
        destination_id,
    )
    .fetch_one(pool)
    .await?;

    Ok(record.exists)
}

#[cfg(test)]
mod tests {
    use aws_lc_rs::aead::RandomizedNonceKey;
    use config::shared::DestinationConfig;
    use config::SerializableSecretString;

    use crate::db::destinations::EncryptedDestinationConfig;
    use crate::db::serde::{decrypt_and_deserialize_from_value, encrypt_and_serialize};
    use crate::encryption::EncryptionKey;

    #[test]
    pub fn destination_config_json_deserialization() {
        let config = r#"{
            "big_query": {
                "project_id": "project-id",
                "dataset_id": "dataset-id",
                "service_account_key": "service-account-key",
                "max_staleness_mins": 42
            }
        }"#;

        let deserialized = serde_json::from_str::<DestinationConfig>(config);
        insta::assert_debug_snapshot!(deserialized.unwrap());
    }

    #[test]
    pub fn destination_config_json_serialization() {
        let config = DestinationConfig::BigQuery {
            project_id: "project-id".to_string(),
            dataset_id: "dataset-id".to_string(),
            service_account_key: SerializableSecretString::from("service-account-key".to_string()),
            max_staleness_mins: Some(42),
        };

        insta::assert_json_snapshot!(config);
    }

    #[test]
    pub fn destination_config_json_encryption() {
        let key_bytes = [42u8; 32];
        let key = RandomizedNonceKey::new(&aws_lc_rs::aead::AES_256_GCM, &key_bytes).unwrap();
        let encryption_key = EncryptionKey { id: 1, key };

        let config = DestinationConfig::BigQuery {
            project_id: "project-id".to_string(),
            dataset_id: "dataset-id".to_string(),
            service_account_key: SerializableSecretString::from("supersecretkey".to_string()),
            max_staleness_mins: Some(99),
        };

        let config_in_db = encrypt_and_serialize::<DestinationConfig, EncryptedDestinationConfig>(
            config.clone(),
            &encryption_key,
        )
        .unwrap();
        insta::assert_json_snapshot!(config_in_db, {
            ".big_query.service_account_key" => "[key]"
        });

        let deserialized_config = decrypt_and_deserialize_from_value::<
            EncryptedDestinationConfig,
            DestinationConfig,
        >(config_in_db, &encryption_key)
        .unwrap();
        insta::assert_debug_snapshot!(deserialized_config);
    }
}
