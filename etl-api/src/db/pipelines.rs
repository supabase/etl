use etl_config::shared::BatchConfig;
use serde::{Deserialize, Serialize};
use sqlx::{PgExecutor, PgTransaction};
use std::ops::DerefMut;
use thiserror::Error;
use utoipa::ToSchema;

use crate::db::replicators::{ReplicatorsDbError, create_replicator};
use crate::db::serde::{
    DbDeserializationError, DbSerializationError, deserialize_from_value, serialize,
};

/// Pipeline configuration used during replication. This struct's fields
/// should be kept in sync with [`OptionalPipelineConfig`]. If a new optional
/// field is added, it should also be included in the [`OptionalPipelineConfig::merge`]
/// implementation.
///
/// A separate struct was created because `publication_name` is not optional and
/// when updating config we do not want the user to pass publication
/// name.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PipelineConfig {
    #[schema(example = "my_publication")]
    pub publication_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<BatchConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_error_retry_delay_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 4)]
    pub max_table_sync_workers: Option<u16>,
}

/// Has the same fields as [`PipelineConfig`] except from
/// the required fields. These two structs should be kept
/// in sync.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OptionalPipelineConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<BatchConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_error_retry_delay_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_table_sync_workers: Option<u16>,
}

impl PipelineConfig {
    /// Merges an [`OptionalPipelineConfig`] into this one by overwriting the optional
    /// fields in self if they are set in the other config. There is currently no
    /// way to unset fields in self, but should be good enough for now.
    fn merge(&mut self, other: OptionalPipelineConfig) {
        if let Some(batch) = other.batch {
            self.batch = Some(batch);
        }

        if let Some(table_error_retry_delay_ms) = other.table_error_retry_delay_ms {
            self.table_error_retry_delay_ms = Some(table_error_retry_delay_ms);
        }

        if let Some(max_table_sync_workers) = other.max_table_sync_workers {
            self.max_table_sync_workers = Some(max_table_sync_workers);
        }
    }
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
    txn: &mut PgTransaction<'_>,
    tenant_id: &str,
    source_id: i64,
    destination_id: i64,
    image_id: i64,
    config: PipelineConfig,
) -> Result<i64, PipelinesDbError> {
    let config = serialize(&config)?;

    let replicator_id = create_replicator(txn.deref_mut(), tenant_id, image_id).await?;
    let record = sqlx::query!(
        r#"
        insert into app.pipelines (tenant_id, source_id, destination_id, replicator_id, config)
        values ($1, $2, $3, $4, $5)
        returning id
        "#,
        tenant_id,
        source_id,
        destination_id,
        replicator_id,
        config
    )
    .fetch_one(txn.deref_mut())
    .await?;

    Ok(record.id)
}

pub async fn read_pipeline<'c, E>(
    executor: E,
    tenant_id: &str,
    pipeline_id: i64,
) -> Result<Option<Pipeline>, PipelinesDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        select p.id,
            p.tenant_id,
            source_id,
            s.name as source_name,
            destination_id,
            d.name as destination_name,
            replicator_id,
            p.config
        from app.pipelines p
        join app.sources s on p.source_id = s.id
        join app.destinations d on p.destination_id = d.id
        where p.tenant_id = $1 and p.id = $2
        "#,
        tenant_id,
        pipeline_id,
    )
    .fetch_optional(executor)
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

pub async fn update_pipeline<'c, E>(
    executor: E,
    tenant_id: &str,
    pipeline_id: i64,
    source_id: i64,
    destination_id: i64,
    config: &PipelineConfig,
) -> Result<Option<i64>, PipelinesDbError>
where
    E: PgExecutor<'c>,
{
    let pipeline_config = serialize(config)?;

    let record = sqlx::query!(
        r#"
        update app.pipelines
        set source_id = $1, destination_id = $2, config = $3
        where tenant_id = $4 and id = $5
        returning id
        "#,
        source_id,
        destination_id,
        pipeline_config,
        tenant_id,
        pipeline_id
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_pipeline<'c, E>(
    executor: E,
    tenant_id: &str,
    pipeline_id: i64,
) -> Result<Option<i64>, PipelinesDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        delete from app.pipelines
        where tenant_id = $1 and id = $2
        returning id
        "#,
        tenant_id,
        pipeline_id
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_pipelines<'c, E>(
    executor: E,
    tenant_id: &str,
) -> Result<Vec<Pipeline>, PipelinesDbError>
where
    E: PgExecutor<'c>,
{
    let records = sqlx::query!(
        r#"
        select p.id,
            p.tenant_id,
            source_id,
            s.name as source_name,
            destination_id,
            d.name as destination_name,
            replicator_id,
            p.config
        from app.pipelines p
        join app.sources s on p.source_id = s.id
        join app.destinations d on p.destination_id = d.id
        where p.tenant_id = $1
        "#,
        tenant_id,
    )
    .fetch_all(executor)
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

pub async fn update_pipeline_config(
    txn: &mut PgTransaction<'_>,
    tenant_id: &str,
    pipeline_id: i64,
    request_config: OptionalPipelineConfig,
) -> Result<Option<PipelineConfig>, PipelinesDbError> {
    // We use `select ... for update` to lock the pipeline row being updated
    // to avoid concurrent requests clobbering data from each other
    let record = sqlx::query!(
        r#"
        select p.id,
            p.config
        from app.pipelines p
        where p.tenant_id = $1 and p.id = $2
        for update
        "#,
        tenant_id,
        pipeline_id,
    )
    .fetch_optional(txn.deref_mut())
    .await?;

    match record {
        Some(record) => {
            let mut config_in_db = deserialize_from_value::<PipelineConfig>(record.config)?;
            config_in_db.merge(request_config);
            let updated_config = serialize(config_in_db)?;

            let record = sqlx::query!(
                r#"
                update app.pipelines
                set config = $1
                where tenant_id = $2 and id = $3
                returning config
                "#,
                updated_config,
                tenant_id,
                pipeline_id
            )
            .fetch_optional(txn.deref_mut())
            .await?;

            match record {
                Some(record) => {
                    let config = deserialize_from_value::<PipelineConfig>(record.config)?;
                    Ok(Some(config))
                }
                None => Ok(None),
            }
        }
        None => Ok(None),
    }
}

/// Helper function to check if an sqlx error is a duplicate pipeline constraint violation
pub fn is_duplicate_pipeline_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => {
            // 23505 is PostgreSQL's unique constraint violation code
            // Check for our unique constraint name defined
            // in the migrations/20250605064229_add_unique_constraint_pipelines_source_destination.sql file
            db_err.code().as_deref() == Some("23505")
                && db_err.constraint() == Some("pipelines_tenant_source_destination_unique")
        }
        _ => false,
    }
}
