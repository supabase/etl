use crate::configs::encryption::EncryptionKey;
use crate::configs::pipeline::{FullApiPipelineConfig, StoredPipelineConfig};
use crate::configs::serde::{
    DbDeserializationError, DbSerializationError, deserialize_from_value, serialize,
};
use crate::db;
use crate::db::destinations::Destination;
use crate::db::images::Image;
use crate::db::replicators::{ReplicatorsDbError, create_replicator};
use crate::db::sources::Source;
use crate::routes::pipelines::PipelineError;
use etl_postgres::replication::{health, schema, slots, state, table_mappings};
use etl_postgres::types::TableId;
use sqlx::{PgConnection, PgExecutor, PgPool, PgTransaction};
use std::ops::DerefMut;
use thiserror::Error;

/// Maximum number of pipelines allowed per tenant.
///
/// For now, we keep the maximum to 1, this way, we give us a simpler surface area for breaking changes
/// to the `etl` schema in the source database since only one pipeline will use it.
pub const MAX_PIPELINES_PER_TENANT: i64 = 1;

#[derive(Debug, Clone)]
pub struct Pipeline {
    pub id: i64,
    pub tenant_id: String,
    pub source_id: i64,
    pub source_name: String,
    pub destination_id: i64,
    pub destination_name: String,
    pub replicator_id: i64,
    pub config: StoredPipelineConfig,
}

#[derive(Debug, Clone, Copy)]
pub struct PipelineDeletion {
    pub id: i64,
    pub source_id: i64,
    pub destination_id: i64,
    pub replicator_id: i64,
}

#[derive(Debug, Error)]
pub enum PipelinesDbError {
    #[error("Error while interacting with Postgres for pipelines: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Error while serializing pipeline config: {0}")]
    DbSerialization(#[from] DbSerializationError),

    #[error("Error while deserializing pipeline config: {0}")]
    DbDeserialization(#[from] DbDeserializationError),

    #[error(transparent)]
    ReplicatorsDb(#[from] ReplicatorsDbError),

    #[error("Slot operation failed: {0}")]
    SlotError(#[from] slots::EtlReplicationSlotError),
}

pub async fn count_pipelines_for_tenant<'c, E>(
    executor: E,
    tenant_id: &str,
) -> Result<i64, PipelinesDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        select count(*) as "count!"
        from app.pipelines
        where tenant_id = $1
        "#,
        tenant_id
    )
    .fetch_one(executor)
    .await?;

    Ok(record.count)
}

pub async fn create_pipeline(
    txn: &mut PgTransaction<'_>,
    tenant_id: &str,
    source_id: i64,
    destination_id: i64,
    image_id: i64,
    config: FullApiPipelineConfig,
) -> Result<i64, PipelinesDbError> {
    let config = serialize(StoredPipelineConfig::from(config))?;

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
            let config = deserialize_from_value::<StoredPipelineConfig>(record.config)?;

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
    config: FullApiPipelineConfig,
) -> Result<Option<i64>, PipelinesDbError>
where
    E: PgExecutor<'c>,
{
    let pipeline_config = serialize(StoredPipelineConfig::from(config))?;

    let record = sqlx::query!(
        r#"
        update app.pipelines
        set source_id = $1, destination_id = $2, config = $3, updated_at = now()
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

pub async fn delete_pipeline_api_and_source_state(
    api_connection: &mut PgConnection,
    source_connection: &mut PgConnection,
    tenant_id: &str,
    pipeline: &PipelineDeletion,
) -> Result<Option<Vec<TableId>>, PipelinesDbError> {
    // Delete the pipeline from the main database (this does NOT cascade delete the replicator due to missing constraint).
    delete_pipeline(&mut *api_connection, tenant_id, pipeline.id).await?;

    // Manually delete the replicator since there's no cascade constraint.
    db::replicators::delete_replicator(&mut *api_connection, tenant_id, pipeline.replicator_id)
        .await?;

    // Get all table IDs for this pipeline before deleting state (only if all ETL tables exist).
    let etl_present = health::etl_tables_present(&mut *source_connection).await?;
    let table_ids = if etl_present {
        Some(state::get_pipeline_table_ids(&mut *source_connection, pipeline.id).await?)
    } else {
        None
    };

    // Delete state, schema, and table mappings from the source database, only if ETL tables exist.
    if etl_present {
        let _ =
            state::delete_replication_state_for_all_tables(&mut *source_connection, pipeline.id)
                .await?;
        let _ = schema::delete_table_schemas_for_all_tables(&mut *source_connection, pipeline.id)
            .await?;
        let _ = table_mappings::delete_table_mappings_for_all_tables(
            &mut *source_connection,
            pipeline.id,
        )
        .await?;
    }

    Ok(table_ids)
}

pub async fn delete_pipelines_api_and_source_state(
    api_connection: &mut PgConnection,
    source_connection: &mut PgConnection,
    tenant_id: &str,
    pipelines: &[PipelineDeletion],
) -> Result<Vec<(i64, Option<Vec<TableId>>)>, PipelinesDbError> {
    let mut pipeline_slot_state = Vec::with_capacity(pipelines.len());
    for pipeline in pipelines {
        let table_ids = delete_pipeline_api_and_source_state(
            api_connection,
            source_connection,
            tenant_id,
            pipeline,
        )
        .await?;
        pipeline_slot_state.push((pipeline.id, table_ids));
    }

    Ok(pipeline_slot_state)
}

pub async fn delete_pipeline_replication_slots(
    source_pool: &PgPool,
    pipeline_id: i64,
    table_ids: Option<Vec<TableId>>,
) -> Result<(), PipelinesDbError> {
    let table_ids = table_ids.as_deref().unwrap_or(&[]);
    slots::delete_pipeline_replication_slots(source_pool, pipeline_id as u64, table_ids).await?;

    Ok(())
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
        let config = deserialize_from_value::<StoredPipelineConfig>(record.config)?;

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

pub async fn read_pipeline_for_deletion<'c, E>(
    executor: E,
    tenant_id: &str,
    pipeline_id: i64,
) -> Result<Option<PipelineDeletion>, PipelinesDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        select id, source_id, destination_id, replicator_id
        from app.pipelines
        where tenant_id = $1 and id = $2
        "#,
        tenant_id,
        pipeline_id,
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|record| PipelineDeletion {
        id: record.id,
        source_id: record.source_id,
        destination_id: record.destination_id,
        replicator_id: record.replicator_id,
    }))
}

pub async fn read_all_pipelines_for_deletion<'c, E>(
    executor: E,
    tenant_id: &str,
) -> Result<Vec<PipelineDeletion>, PipelinesDbError>
where
    E: PgExecutor<'c>,
{
    let records = sqlx::query!(
        r#"
        select id, source_id, destination_id, replicator_id
        from app.pipelines
        where tenant_id = $1
        "#,
        tenant_id,
    )
    .fetch_all(executor)
    .await?;

    Ok(records
        .into_iter()
        .map(|record| PipelineDeletion {
            id: record.id,
            source_id: record.source_id,
            destination_id: record.destination_id,
            replicator_id: record.replicator_id,
        })
        .collect())
}

pub async fn read_pipelines_for_source_for_deletion<'c, E>(
    executor: E,
    tenant_id: &str,
    source_id: i64,
) -> Result<Vec<PipelineDeletion>, PipelinesDbError>
where
    E: PgExecutor<'c>,
{
    let records = sqlx::query!(
        r#"
        select id, source_id, destination_id, replicator_id
        from app.pipelines
        where tenant_id = $1 and source_id = $2
        "#,
        tenant_id,
        source_id,
    )
    .fetch_all(executor)
    .await?;

    Ok(records
        .into_iter()
        .map(|record| PipelineDeletion {
            id: record.id,
            source_id: record.source_id,
            destination_id: record.destination_id,
            replicator_id: record.replicator_id,
        })
        .collect())
}

pub async fn read_pipelines_for_destination_for_deletion<'c, E>(
    executor: E,
    tenant_id: &str,
    destination_id: i64,
) -> Result<Vec<PipelineDeletion>, PipelinesDbError>
where
    E: PgExecutor<'c>,
{
    let records = sqlx::query!(
        r#"
        select id, source_id, destination_id, replicator_id
        from app.pipelines
        where tenant_id = $1 and destination_id = $2
        "#,
        tenant_id,
        destination_id,
    )
    .fetch_all(executor)
    .await?;

    Ok(records
        .into_iter()
        .map(|record| PipelineDeletion {
            id: record.id,
            source_id: record.source_id,
            destination_id: record.destination_id,
            replicator_id: record.replicator_id,
        })
        .collect())
}

pub async fn read_pipeline_components(
    txn: &mut PgTransaction<'_>,
    tenant_id: &str,
    pipeline_id: i64,
    encryption_key: &EncryptionKey,
) -> Result<
    (
        Pipeline,
        db::replicators::Replicator,
        Image,
        Source,
        Destination,
    ),
    PipelineError,
> {
    let pipeline = read_pipeline(txn.deref_mut(), tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    let replicator =
        db::replicators::read_replicator_by_pipeline_id(txn.deref_mut(), tenant_id, pipeline_id)
            .await?
            .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    let image = db::images::read_image_by_replicator_id(txn.deref_mut(), replicator.id)
        .await?
        .ok_or(PipelineError::ImageNotFound(replicator.id))?;

    let source = db::sources::read_source(
        txn.deref_mut(),
        tenant_id,
        pipeline.source_id,
        encryption_key,
    )
    .await?
    .ok_or(PipelineError::SourceNotFound(pipeline.source_id))?;

    let destination = db::destinations::read_destination(
        txn.deref_mut(),
        tenant_id,
        pipeline.destination_id,
        encryption_key,
    )
    .await?
    .ok_or(PipelineError::DestinationNotFound(pipeline.destination_id))?;

    Ok((pipeline, replicator, image, source, destination))
}
