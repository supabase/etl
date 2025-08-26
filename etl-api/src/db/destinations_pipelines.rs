use sqlx::PgTransaction;
use std::ops::DerefMut;
use thiserror::Error;

use crate::configs::destination::FullApiDestinationConfig;
use crate::configs::encryption::EncryptionKey;
use crate::configs::pipeline::FullApiPipelineConfig;
use crate::configs::serde::{DbDeserializationError, DbSerializationError};
use crate::db::destinations::{DestinationsDbError, create_destination, update_destination};
use crate::db::pipelines::{PipelinesDbError, create_pipeline, update_pipeline};

#[derive(Debug, Error)]
pub enum DestinationPipelinesDbError {
    #[error("Error while interacting with Postgres for destination and/or pipelines: {0}")]
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
    txn: &mut PgTransaction<'_>,
    tenant_id: &str,
    source_id: i64,
    destination_name: &str,
    destination_config: FullApiDestinationConfig,
    image_id: i64,
    pipeline_config: FullApiPipelineConfig,
    encryption_key: &EncryptionKey,
) -> Result<(i64, i64), DestinationPipelinesDbError> {
    let destination_id = create_destination(
        txn.deref_mut(),
        tenant_id,
        destination_name,
        destination_config,
        encryption_key,
    )
    .await?;

    let pipeline_id = create_pipeline(
        txn,
        tenant_id,
        source_id,
        destination_id,
        image_id,
        pipeline_config,
    )
    .await?;

    Ok((destination_id, pipeline_id))
}

#[expect(clippy::too_many_arguments)]
pub async fn update_destination_and_pipeline(
    mut txn: PgTransaction<'_>,
    tenant_id: &str,
    destination_id: i64,
    pipeline_id: i64,
    source_id: i64,
    destination_name: &str,
    destination_config: FullApiDestinationConfig,
    pipeline_config: FullApiPipelineConfig,
    encryption_key: &EncryptionKey,
) -> Result<(), DestinationPipelinesDbError> {
    let destination_id_res = update_destination(
        txn.deref_mut(),
        tenant_id,
        destination_name,
        destination_id,
        destination_config,
        encryption_key,
    )
    .await?;

    if destination_id_res.is_none() {
        txn.rollback().await?;
        return Err(DestinationPipelinesDbError::DestinationNotFound(
            destination_id,
        ));
    };

    let pipeline_id_res = update_pipeline(
        txn.deref_mut(),
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
