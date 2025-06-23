use config::shared::DestinationConfig;
use sqlx::PgPool;
use thiserror::Error;

use crate::db::base::{
    encrypt_and_serialize, serialize, DbDeserializationError, DbSerializationError,
};
use crate::db::destinations::{
    create_destination_txn, update_destination_txn, DestinationsDbError,
};
use crate::db::pipelines::{
    create_pipeline_txn, update_pipeline_txn, PipelineConfig, PipelinesDbError,
};
use crate::encryption::EncryptionKey;

#[derive(Debug, Error)]
pub enum DestinationPipelineDbError {
    #[error("Error while interacting with PostgreSQL for sources: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("The destination with id {0} was not found")]
    DestinationNotFound(i64),

    #[error("The pipeline with id {0} was not found")]
    PipelineNotFound(i64),

    #[error("Error while interacting with a pipeline: {0}")]
    PipelinesDb(#[from] PipelinesDbError),

    #[error("Error while interacting with a destination: {0}")]
    DestinationsDb(#[from] DestinationsDbError),

    #[error("Error while serializing destination or source config: {0}")]
    DbSerializationError(#[from] DbSerializationError),

    #[error("Error while deserializing destination or pipeline config: {0}")]
    DbDeserializationError(#[from] DbDeserializationError),
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
) -> Result<(i64, i64), DestinationPipelineDbError> {
    let destination_config = encrypt_and_serialize(destination_config, encryption_key)?;
    let publication_name = pipeline_config.publication_name.clone();
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
        &publication_name,
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
) -> Result<(), DestinationPipelineDbError> {
    let destination_config = encrypt_and_serialize(destination_config, encryption_key)?;
    let publication_name = pipeline_config.publication_name.clone();
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
        return Err(DestinationPipelineDbError::DestinationNotFound(
            destination_id,
        ));
    };
    let pipeline_id_res = update_pipeline_txn(
        &mut txn,
        tenant_id,
        pipeline_id,
        source_id,
        destination_id,
        &publication_name,
        pipeline_config,
    )
    .await?;

    if pipeline_id_res.is_none() {
        txn.rollback().await?;
        return Err(DestinationPipelineDbError::PipelineNotFound(pipeline_id));
    };

    txn.commit().await?;

    Ok(())
}
