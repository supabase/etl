use etl_postgres::replication::{schema, slots, state, table_mappings};
use sqlx::{PgExecutor, PgTransaction};
use std::ops::DerefMut;
use thiserror::Error;

use crate::configs::pipeline::{
    FullApiPipelineConfig, PartialApiPipelineConfig, StoredPipelineConfig,
};
use crate::configs::serde::{
    DbDeserializationError, DbSerializationError, deserialize_from_value, serialize,
};
use crate::db;
use crate::db::destinations::{Destination, DestinationsDbError};
use crate::db::replicators::{ReplicatorsDbError, create_replicator};
use crate::db::sources::Source;
use crate::routes::connect_to_source_database_with_defaults;

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

    #[error(transparent)]
    DestinationsDb(#[from] DestinationsDbError),

    #[error("Slot operation failed: {0}")]
    SlotError(#[from] slots::SlotError),
}

pub async fn create_pipeline(
    txn: &mut PgTransaction<'_>,
    tenant_id: &str,
    source_id: i64,
    destination_id: i64,
    image_id: i64,
    config: FullApiPipelineConfig,
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
    config: &FullApiPipelineConfig,
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

pub async fn delete_pipeline_cascading(
    mut txn: PgTransaction<'_>,
    tenant_id: &str,
    pipeline: &Pipeline,
    source: &Source,
    destination: Option<&Destination>,
) -> Result<(), PipelinesDbError> {
    let source_pool =
        connect_to_source_database_with_defaults(&source.config.clone().into_connection_config())
            .await?;

    // We start a transaction in the source database while the other transaction is active in the
    // api database so that in case of failures when deleting the state, we also rollback the transaction
    // in the api database.
    let mut source_txn = source_pool.begin().await?;

    // Delete the pipeline from the main database (this does NOT cascade delete the replicator due to missing constraint)
    delete_pipeline(txn.deref_mut(), tenant_id, pipeline.id).await?;

    // Manually delete the replicator since there's no cascade constraint
    db::replicators::delete_replicator(txn.deref_mut(), tenant_id, pipeline.replicator_id).await?;

    // If a destination is supplied, also the destination will be deleted.
    if let Some(destination) = destination {
        db::destinations::delete_destination(txn.deref_mut(), tenant_id, destination.id).await?;
    }

    // Get all table IDs for this pipeline before deleting state.
    let table_ids = state::get_pipeline_table_ids(source_txn.deref_mut(), pipeline.id).await?;

    // Delete state, schema, and table mappings from the source database
    state::delete_pipeline_replication_state(source_txn.deref_mut(), pipeline.id).await?;
    schema::delete_pipeline_table_schemas(source_txn.deref_mut(), pipeline.id).await?;
    table_mappings::delete_pipeline_table_mappings(source_txn.deref_mut(), pipeline.id).await?;

    // Here we finish `txn` before `source_txn` since we want the guarantee that the pipeline has
    // been deleted before committing the state and slots deletions.
    txn.commit().await?;
    source_txn.commit().await?;

    // If we succeeded to commit both transactions, we are safe to delete the slots. The reason for
    // not deleting slots in the transaction is that `pg_drop_replication_slot(...)` is not transactional.
    slots::delete_pipeline_replication_slots(&source_pool, pipeline.id as u64, &table_ids).await?;

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
        let config = deserialize_from_value::<StoredPipelineConfig>(record.config.clone())?;

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
    config: PartialApiPipelineConfig,
) -> Result<Option<StoredPipelineConfig>, PipelinesDbError> {
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
            let mut config_in_db = deserialize_from_value::<StoredPipelineConfig>(record.config)?;
            config_in_db.merge(config);

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
                    let config = deserialize_from_value::<StoredPipelineConfig>(record.config)?;
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
            // 23505 is Postgres's unique constraint violation code
            // Check for our unique constraint name defined
            // in the migrations/20250605064229_add_unique_constraint_pipelines_source_destination.sql file
            db_err.code().as_deref() == Some("23505")
                && db_err.constraint() == Some("pipelines_tenant_source_destination_unique")
        }
        _ => false,
    }
}
