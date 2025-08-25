use sqlx::PgTransaction;
use std::ops::DerefMut;
use thiserror::Error;

use crate::configs::encryption::EncryptionKey;
use crate::configs::serde::DbSerializationError;
use crate::configs::source::FullApiSourceConfig;
use crate::db::sources::{SourcesDbError, create_source};
use crate::db::tenants::{TenantsDbError, create_tenant};

#[derive(Debug, Error)]
pub enum TenantSourceDbError {
    #[error("Error while interacting with Postgres for tenants and/or sources: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Error while serializing tenant or source config: {0}")]
    DbSerialization(#[from] DbSerializationError),

    #[error(transparent)]
    Sources(#[from] SourcesDbError),

    #[error(transparent)]
    Tenants(#[from] TenantsDbError),
}

pub async fn create_tenant_and_source(
    txn: &mut PgTransaction<'_>,
    tenant_id: &str,
    tenant_name: &str,
    source_name: &str,
    source_config: FullApiSourceConfig,
    encryption_key: &EncryptionKey,
) -> Result<(String, i64), TenantSourceDbError> {
    let tenant_id = create_tenant(txn.deref_mut(), tenant_id, tenant_name).await?;
    let source_id = create_source(
        txn.deref_mut(),
        &tenant_id,
        source_name,
        source_config,
        encryption_key,
    )
    .await?;

    Ok((tenant_id, source_id))
}
