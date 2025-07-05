use sqlx::PgPool;
use thiserror::Error;

use crate::db::serde::{DbSerializationError, encrypt_and_serialize};
use crate::db::sources::{SourceConfig, SourcesDbError, create_source_txn};
use crate::db::tenants::{TenantsDbError, create_tenant_txn};
use crate::encryption::EncryptionKey;

#[derive(Debug, Error)]
pub enum TenantSourceDbError {
    #[error("Error while interacting with PostgreSQL for tenants and/or sources: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Error while serializing tenant or source config: {0}")]
    DbSerialization(#[from] DbSerializationError),

    #[error(transparent)]
    Sources(#[from] SourcesDbError),

    #[error(transparent)]
    Tenants(#[from] TenantsDbError),
}

pub async fn create_tenant_and_source(
    pool: &PgPool,
    tenant_id: &str,
    tenant_name: &str,
    source_name: &str,
    source_config: SourceConfig,
    encryption_key: &EncryptionKey,
) -> Result<(String, i64), TenantSourceDbError> {
    let source_config = encrypt_and_serialize(source_config, encryption_key)?;

    let mut txn = pool.begin().await?;
    let tenant_id = create_tenant_txn(&mut txn, tenant_id, tenant_name).await?;
    let source_id = create_source_txn(&mut txn, &tenant_id, source_name, source_config).await?;
    txn.commit().await?;

    Ok((tenant_id, source_id))
}
