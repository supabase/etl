use sqlx::PgExecutor;
use std::fmt::Debug;
use thiserror::Error;

use crate::configs::destination::{
    EncryptedStoredDestinationConfig, FullApiDestinationConfig, StoredDestinationConfig,
};
use crate::configs::encryption::EncryptionKey;
use crate::configs::serde::{
    DbDeserializationError, DbSerializationError, decrypt_and_deserialize_from_value,
    encrypt_and_serialize,
};

#[derive(Debug, Error)]
pub enum DestinationsDbError {
    #[error("Error while interacting with Postgres for destinations: {0}")]
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
    pub config: StoredDestinationConfig,
}

pub async fn create_destination<'c, E>(
    executor: E,
    tenant_id: &str,
    name: &str,
    config: FullApiDestinationConfig,
    encryption_key: &EncryptionKey,
) -> Result<i64, DestinationsDbError>
where
    E: PgExecutor<'c>,
{
    let config = encrypt_and_serialize(StoredDestinationConfig::from(config), encryption_key)?;

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
    .fetch_one(executor)
    .await?;

    Ok(record.id)
}

pub async fn read_destination<'c, E>(
    executor: E,
    tenant_id: &str,
    destination_id: i64,
    encryption_key: &EncryptionKey,
) -> Result<Option<Destination>, DestinationsDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        select id, tenant_id, name, config
        from app.destinations
        where tenant_id = $1 and id = $2
        "#,
        tenant_id,
        destination_id,
    )
    .fetch_optional(executor)
    .await?;

    let destination = match record {
        Some(record) => {
            let config = decrypt_and_deserialize_from_value::<
                EncryptedStoredDestinationConfig,
                StoredDestinationConfig,
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

pub async fn update_destination<'c, E>(
    executor: E,
    tenant_id: &str,
    name: &str,
    destination_id: i64,
    config: FullApiDestinationConfig,
    encryption_key: &EncryptionKey,
) -> Result<Option<i64>, DestinationsDbError>
where
    E: PgExecutor<'c>,
{
    let config = encrypt_and_serialize(StoredDestinationConfig::from(config), encryption_key)?;

    let record = sqlx::query!(
        r#"
        update app.destinations
        set config = $1, name = $2, updated_at = now()
        where tenant_id = $3 and id = $4
        returning id
        "#,
        config,
        name,
        tenant_id,
        destination_id
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_destination<'c, E>(
    executor: E,
    tenant_id: &str,
    destination_id: i64,
) -> Result<Option<i64>, DestinationsDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        delete from app.destinations
        where tenant_id = $1 and id = $2
        returning id
        "#,
        tenant_id,
        destination_id
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_destinations<'c, E>(
    executor: E,
    tenant_id: &str,
    encryption_key: &EncryptionKey,
) -> Result<Vec<Destination>, DestinationsDbError>
where
    E: PgExecutor<'c>,
{
    let records = sqlx::query!(
        r#"
        select id, tenant_id, name, config
        from app.destinations
        where tenant_id = $1
        "#,
        tenant_id,
    )
    .fetch_all(executor)
    .await?;

    let mut destinations = Vec::with_capacity(records.len());
    for record in records {
        let config = decrypt_and_deserialize_from_value::<
            EncryptedStoredDestinationConfig,
            StoredDestinationConfig,
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

pub async fn destination_exists<'c, E>(
    executor: E,
    tenant_id: &str,
    destination_id: i64,
) -> Result<bool, DestinationsDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        select exists (select id
        from app.destinations
        where tenant_id = $1 and id = $2) as "exists!"
        "#,
        tenant_id,
        destination_id,
    )
    .fetch_one(executor)
    .await?;

    Ok(record.exists)
}
