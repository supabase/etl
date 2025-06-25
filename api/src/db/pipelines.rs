use config::shared::{BatchConfig, RetryConfig};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Transaction};
use thiserror::Error;

use crate::db::replicators::{create_replicator_txn, ReplicatorsDbError};
use crate::db::serde::{
    deserialize_from_value, serialize, DbDeserializationError, DbSerializationError,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub publication_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<BatchConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub apply_worker_init_retry: Option<RetryConfig>,
}

pub struct Pipeline {
    pub id: i64,
    pub tenant_id: String,
    pub source_id: i64,
    pub source_name: String,
    pub destination_id: i64,
    pub destination_name: String,
    pub replicator_id: i64,
    pub config: PipelineConfig,
}

#[derive(Debug, Error)]
pub enum PipelinesDbError {
    #[error("Error while interacting with PostgreSQL for pipelines: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Error while serializing pipeline config: {0}")]
    DbSerialization(#[from] DbSerializationError),

    #[error("Error while deserializing pipeline config: {0}")]
    DbDeserialization(#[from] DbDeserializationError),

    #[error(transparent)]
    ReplicatorsDb(#[from] ReplicatorsDbError),
}

pub async fn create_pipeline(
    pool: &PgPool,
    tenant_id: &str,
    source_id: i64,
    destination_id: i64,
    image_id: i64,
    config: PipelineConfig,
) -> Result<i64, PipelinesDbError> {
    // TODO: remove publication name since it's in the config (only when the db column is removed).
    let publication_name = &config.publication_name;
    let config = serialize(&config)?;
    let config = serde_json::to_value(config).expect("failed to serialize config");

    let mut txn = pool.begin().await?;
    let res = create_pipeline_txn(
        &mut txn,
        tenant_id,
        source_id,
        destination_id,
        image_id,
        publication_name,
        config,
    )
    .await;
    txn.commit().await?;

    res
}

pub async fn create_pipeline_txn(
    txn: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    source_id: i64,
    destination_id: i64,
    image_id: i64,
    publication_name: &str,
    pipeline_config: serde_json::Value,
) -> Result<i64, PipelinesDbError> {
    let replicator_id = create_replicator_txn(txn, tenant_id, image_id).await?;
    let record = sqlx::query!(
        r#"
        insert into app.pipelines (tenant_id, source_id, destination_id, replicator_id, publication_name, config)
        values ($1, $2, $3, $4, $5, $6)
        returning id
        "#,
        tenant_id,
        source_id,
        destination_id,
        replicator_id,
        publication_name,
        pipeline_config
    )
    .fetch_one(&mut **txn)
    .await?;

    Ok(record.id)
}

pub async fn read_pipeline(
    pool: &PgPool,
    tenant_id: &str,
    pipeline_id: i64,
) -> Result<Option<Pipeline>, PipelinesDbError> {
    let record = sqlx::query!(
        r#"
        select p.id,
            p.tenant_id,
            source_id,
            s.name as source_name,
            destination_id,
            d.name as destination_name,
            replicator_id,
            publication_name,
            p.config
        from app.pipelines p
        join app.sources s on p.source_id = s.id
        join app.destinations d on p.destination_id = d.id
        where p.tenant_id = $1 and p.id = $2
        "#,
        tenant_id,
        pipeline_id,
    )
    .fetch_optional(pool)
    .await?;

    let pipeline = match record {
        Some(record) => {
            let config = deserialize_from_value::<PipelineConfig>(record.config)?;

            let pipeline = Pipeline {
                id: record.id,
                tenant_id: record.tenant_id,
                source_id: record.source_id,
                source_name: record.source_name,
                destination_id: record.destination_id,
                destination_name: record.destination_name,
                replicator_id: record.replicator_id,
                config,
            };

            Some(pipeline)
        }
        None => None,
    };

    Ok(pipeline)
}

pub async fn update_pipeline(
    pool: &PgPool,
    tenant_id: &str,
    pipeline_id: i64,
    source_id: i64,
    destination_id: i64,
    config: &PipelineConfig,
) -> Result<Option<i64>, PipelinesDbError> {
    let publication_name = &config.publication_name;
    let pipeline_config = serialize(config)?;

    let mut txn = pool.begin().await?;
    let res = update_pipeline_txn(
        &mut txn,
        tenant_id,
        pipeline_id,
        source_id,
        destination_id,
        publication_name,
        pipeline_config,
    )
    .await?;
    txn.commit().await?;

    Ok(res)
}

pub async fn update_pipeline_txn(
    txn: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    pipeline_id: i64,
    source_id: i64,
    destination_id: i64,
    publication_name: &str,
    pipeline_config: serde_json::Value,
) -> Result<Option<i64>, PipelinesDbError> {
    let record = sqlx::query!(
        r#"
        update app.pipelines
        set source_id = $1, destination_id = $2, publication_name = $3, config = $4
        where tenant_id = $5 and id = $6
        returning id
        "#,
        source_id,
        destination_id,
        publication_name,
        pipeline_config,
        tenant_id,
        pipeline_id
    )
    .fetch_optional(&mut **txn)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_pipeline(
    pool: &PgPool,
    tenant_id: &str,
    pipeline_id: i64,
) -> Result<Option<i64>, PipelinesDbError> {
    let record = sqlx::query!(
        r#"
        delete from app.pipelines
        where tenant_id = $1 and id = $2
        returning id
        "#,
        tenant_id,
        pipeline_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_pipelines(
    pool: &PgPool,
    tenant_id: &str,
) -> Result<Vec<Pipeline>, PipelinesDbError> {
    let records = sqlx::query!(
        r#"
        select p.id,
            p.tenant_id,
            source_id,
            s.name as source_name,
            destination_id,
            d.name as destination_name,
            replicator_id,
            publication_name,
            p.config
        from app.pipelines p
        join app.sources s on p.source_id = s.id
        join app.destinations d on p.destination_id = d.id
        where p.tenant_id = $1
        "#,
        tenant_id,
    )
    .fetch_all(pool)
    .await?;

    let mut pipelines = Vec::with_capacity(records.len());
    for record in records {
        let config = deserialize_from_value::<PipelineConfig>(record.config.clone())
            .expect("failed to deserialize pipeline config");

        pipelines.push(Pipeline {
            id: record.id,
            tenant_id: record.tenant_id,
            source_id: record.source_id,
            source_name: record.source_name,
            destination_id: record.destination_id,
            destination_name: record.destination_name,
            replicator_id: record.replicator_id,
            config,
        });
    }

    Ok(pipelines)
}

/// Helper function to check if an sqlx error is a duplicate pipeline constraint violation
pub fn is_duplicate_pipeline_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(ref db_err) => {
            // 23505 is PostgreSQL's unique constraint violation code
            // Check for our unique constraint name defined
            // in the migrations/20250605064229_add_unique_constraint_pipelines_source_destination.sql file
            db_err.code().as_deref() == Some("23505")
                && db_err.constraint() == Some("pipelines_tenant_source_destination_unique")
        }
        _ => false,
    }
}
