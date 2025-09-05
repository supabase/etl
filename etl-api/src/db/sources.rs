use sqlx::PgExecutor;
use std::fmt::Debug;
use thiserror::Error;

use crate::configs::encryption::EncryptionKey;
use crate::configs::serde::{
    DbDeserializationError, DbSerializationError, decrypt_and_deserialize_from_value,
    encrypt_and_serialize,
};
use crate::configs::source::{
    EncryptedStoredSourceConfig, FullApiSourceConfig, StoredSourceConfig,
};

#[derive(Debug)]
pub struct Source {
    pub id: i64,
    pub tenant_id: String,
    pub name: String,
    pub config: StoredSourceConfig,
}

#[derive(Debug, Error)]
pub enum SourcesDbError {
    #[error("Error while interacting with Postgres for sources: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Error while serializing source config: {0}")]
    DbSerialization(#[from] DbSerializationError),

    #[error("Error while deserializing source config: {0}")]
    DbDeserialization(#[from] DbDeserializationError),
}

pub async fn create_source<'c, E>(
    executor: E,
    tenant_id: &str,
    name: &str,
    config: FullApiSourceConfig,
    encryption_key: &EncryptionKey,
) -> Result<i64, SourcesDbError>
where
    E: PgExecutor<'c>,
{
    let config = encrypt_and_serialize::<StoredSourceConfig, EncryptedStoredSourceConfig>(
        StoredSourceConfig::from(config),
        encryption_key,
    )?;

    let record = sqlx::query!(
        r#"
        insert into app.sources (tenant_id, name, config)
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

pub async fn read_source<'c, E>(
    executor: E,
    tenant_id: &str,
    source_id: i64,
    encryption_key: &EncryptionKey,
) -> Result<Option<Source>, SourcesDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        select id, tenant_id, name, config
        from app.sources
        where tenant_id = $1 and id = $2
        "#,
        tenant_id,
        source_id,
    )
    .fetch_optional(executor)
    .await?;

    let source = match record {
        Some(record) => {
            let config = decrypt_and_deserialize_from_value::<
                EncryptedStoredSourceConfig,
                StoredSourceConfig,
            >(record.config, encryption_key)?;

            Some(Source {
                id: record.id,
                tenant_id: record.tenant_id,
                name: record.name,
                config,
            })
        }
        None => None,
    };

    Ok(source)
}

pub async fn update_source<'c, E>(
    executor: E,
    tenant_id: &str,
    name: &str,
    source_id: i64,
    config: FullApiSourceConfig,
    encryption_key: &EncryptionKey,
) -> Result<Option<i64>, SourcesDbError>
where
    E: PgExecutor<'c>,
{
    let config = encrypt_and_serialize::<StoredSourceConfig, EncryptedStoredSourceConfig>(
        StoredSourceConfig::from(config),
        encryption_key,
    )?;

    let record = sqlx::query!(
        r#"
        update app.sources
        set config = $1, name = $2, updated_at = now()
        where tenant_id = $3 and id = $4
        returning id
        "#,
        config,
        name,
        tenant_id,
        source_id
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_source<'c, E>(
    executor: E,
    tenant_id: &str,
    source_id: i64,
) -> Result<Option<i64>, SourcesDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        delete from app.sources
        where tenant_id = $1 and id = $2
        returning id
        "#,
        tenant_id,
        source_id
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_sources<'c, E>(
    executor: E,
    tenant_id: &str,
    encryption_key: &EncryptionKey,
) -> Result<Vec<Source>, SourcesDbError>
where
    E: PgExecutor<'c>,
{
    let records = sqlx::query!(
        r#"
        select id, tenant_id, name, config
        from app.sources
        where tenant_id = $1
        "#,
        tenant_id,
    )
    .fetch_all(executor)
    .await?;

    let mut sources = Vec::with_capacity(records.len());
    for record in records {
        let config = decrypt_and_deserialize_from_value::<
            EncryptedStoredSourceConfig,
            StoredSourceConfig,
        >(record.config.clone(), encryption_key)?;
        let source = Source {
            id: record.id,
            tenant_id: record.tenant_id,
            name: record.name,
            config,
        };
        sources.push(source);
    }

    Ok(sources)
}

pub async fn source_exists<'c, E>(
    executor: E,
    tenant_id: &str,
    source_id: i64,
) -> Result<bool, SourcesDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        select exists (select id
        from app.sources
        where tenant_id = $1 and id = $2) as "exists!"
        "#,
        tenant_id,
        source_id
    )
    .fetch_one(executor)
    .await?;

    Ok(record.exists)
}
