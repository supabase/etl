use sqlx::PgExecutor;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReplicatorsDbError {
    #[error("Error while interacting with Postgres for replicators: {0}")]
    Database(#[from] sqlx::Error),
}

pub struct Replicator {
    pub id: i64,
    pub tenant_id: String,
    pub image_id: i64,
}

pub async fn create_replicator<'c, E>(
    executor: E,
    tenant_id: &str,
    image_id: i64,
) -> Result<i64, ReplicatorsDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        insert into app.replicators (tenant_id, image_id)
        values ($1, $2)
        returning id
        "#,
        tenant_id,
        image_id
    )
    .fetch_one(executor)
    .await?;

    Ok(record.id)
}

pub async fn read_replicator_by_pipeline_id<'c, E>(
    executor: E,
    tenant_id: &str,
    pipeline_id: i64,
) -> Result<Option<Replicator>, ReplicatorsDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        select r.id, r.tenant_id, r.image_id
        from app.replicators r
        join app.pipelines p on r.id = p.replicator_id
        where r.tenant_id = $1 and p.tenant_id = $1 and p.id = $2
        "#,
        tenant_id,
        pipeline_id,
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| Replicator {
        id: r.id,
        tenant_id: r.tenant_id,
        image_id: r.image_id,
    }))
}

pub async fn read_replicators<'c, E>(
    executor: E,
    tenant_id: &str,
) -> Result<Vec<Replicator>, ReplicatorsDbError>
where
    E: PgExecutor<'c>,
{
    let mut records = sqlx::query!(
        r#"
        select r.id, r.tenant_id, r.image_id
        from app.replicators r
        join app.pipelines p on r.id = p.replicator_id
        where r.tenant_id = $1 and p.tenant_id = $1
        "#,
        tenant_id,
    )
    .fetch_all(executor)
    .await?;

    Ok(records
        .drain(..)
        .map(|r| Replicator {
            id: r.id,
            tenant_id: r.tenant_id,
            image_id: r.image_id,
        })
        .collect())
}

pub async fn update_replicator_image<'c, E>(
    executor: E,
    tenant_id: &str,
    replicator_id: i64,
    image_id: i64,
) -> Result<Option<i64>, ReplicatorsDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        update app.replicators
        set image_id = $1, updated_at = now()
        where id = $2 and tenant_id = $3
        returning id
        "#,
        image_id,
        replicator_id,
        tenant_id
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_replicator<'c, E>(
    executor: E,
    tenant_id: &str,
    replicator_id: i64,
) -> Result<Option<i64>, ReplicatorsDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        delete from app.replicators
        where tenant_id = $1 and id = $2
        returning id
        "#,
        tenant_id,
        replicator_id
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| r.id))
}
