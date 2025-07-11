use sqlx::{Executor, PgPool, Postgres};
use thiserror::Error;

use crate::db::serde::DbSerializationError;
use crate::db::sources::{SourceConfig, SourcesDbError, create_source};
use crate::db::tenants::{TenantsDbError, create_tenant};
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

pub async fn create_tenant_and_source<'c, E>(
    executor: E,
    tenant_id: &str,
    tenant_name: &str,
    source_name: &str,
    source_config: SourceConfig,
    encryption_key: &EncryptionKey,
) -> Result<(String, i64), TenantSourceDbError>
where
    E: Executor<'c, Database = Postgres>,
{
    let tenant_id = create_tenant(executor, tenant_id, tenant_name).await?;
    let source_id = create_source(
        executor,
        &tenant_id,
        source_name,
        source_config,
        encryption_key,
    )
    .await?;

    Ok((tenant_id, source_id))
}
