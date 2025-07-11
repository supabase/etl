use config::shared::DestinationConfig;
use sqlx::PgPool;
use thiserror::Error;

use crate::db::destinations::{
    DestinationsDbError, create_destination_txn, update_destination_txn,
};
use crate::db::pipelines::{
    PipelineConfig, PipelinesDbError, create_pipeline_txn, update_pipeline_txn,
};
use crate::db::serde::{
    DbDeserializationError, DbSerializationError, encrypt_and_serialize, serialize,
};
use crate::encryption::EncryptionKey;

#[derive(Debug, Error)]
pub enum DestinationPipelinesDbError {
    #[error("Error while interacting with PostgreSQL for destination and/or pipelines: {0}")]
    Database(#[from] sqlx::Error),

    #[error("The destination with id {0} was not found")]
    DestinationNotFound(i64),

    #[error("The pipeline with id {0} was not found")]
    PipelineNotFound(i64),

    #[error(transparent)]
    PipelinesDb(#[from] PipelinesDbError),

    #[error(transparent)]
    DestinationsDb(#[from] DestinationsDbError),

    #[error("Error while serializing destination or pipeline config: {0}")]
    DbSerialization(#[from] DbSerializationError),

    #[error("Error while deserializing destination or pipeline config: {0}")]
    DbDeserialization(#[from] DbDeserializationError),
}

#[expect(clippy::too_many_arguments)]
pub async fn create_destination_and_pipeline(
    pool: &PgPool,
    tenant_id: &str,
    source_id: i64,
    destination_name: &str,
    destination_config: DestinationConfig,
    image_id: i64,
    pipeline_config: PipelineConfig,
    encryption_key: &EncryptionKey,
) -> Result<(i64, i64), DestinationPipelinesDbError> {
    let destination_config = encrypt_and_serialize(destination_config, encryption_key)?;
    let pipeline_config = serialize(pipeline_config)?;

    let mut txn = pool.begin().await?;
    let destination_id =
        create_destination_txn(&mut txn, tenant_id, destination_name, destination_config).await?;
    let pipeline_id = create_pipeline_txn(
        &mut txn,
        tenant_id,
        source_id,
        destination_id,
        image_id,
        pipeline_config,
    )
    .await?;
    txn.commit().await?;

    Ok((destination_id, pipeline_id))
}

#[expect(clippy::too_many_arguments)]
pub async fn update_destination_and_pipeline(
    pool: &PgPool,
    tenant_id: &str,
    destination_id: i64,
    pipeline_id: i64,
    source_id: i64,
    destination_name: &str,
    destination_config: DestinationConfig,
    pipeline_config: PipelineConfig,
    encryption_key: &EncryptionKey,
) -> Result<(), DestinationPipelinesDbError> {
    let destination_config = encrypt_and_serialize(destination_config, encryption_key)?;
    let pipeline_config = serialize(pipeline_config)?;

    let mut txn = pool.begin().await?;
    let destination_id_res = update_destination_txn(
        &mut txn,
        tenant_id,
        destination_name,
        destination_id,
        destination_config,
    )
    .await?;
    if destination_id_res.is_none() {
        txn.rollback().await?;
        return Err(DestinationPipelinesDbError::DestinationNotFound(
            destination_id,
        ));
    };
    let pipeline_id_res = update_pipeline_txn(
        &mut txn,
        tenant_id,
        pipeline_id,
        source_id,
        destination_id,
        pipeline_config,
    )
    .await?;

    if pipeline_id_res.is_none() {
        txn.rollback().await?;
        return Err(DestinationPipelinesDbError::PipelineNotFound(pipeline_id));
    };

    txn.commit().await?;

    Ok(())
}
