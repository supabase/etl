use sqlx::{PgExecutor, PgPool};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ImagesDbError {
    #[error("Error while interacting with Postgres for images: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Cannot delete the default image")]
    CannotDeleteDefault,
}

pub struct Image {
    pub id: i64,
    pub name: String,
    pub is_default: bool,
}

pub async fn create_image(
    pool: &PgPool,
    name: &str,
    is_default: bool,
) -> Result<i64, ImagesDbError> {
    let mut txn = pool.begin().await?;

    // If this is to be the new default image, first unset any existing default
    if is_default {
        sqlx::query!(
            r#"
            update app.images
            set is_default = false
            where is_default = true
            "#
        )
        .execute(&mut *txn)
        .await?;
    }

    let record = sqlx::query!(
        r#"
        insert into app.images (name, is_default)
        values ($1, $2)
        returning id
        "#,
        name,
        is_default
    )
    .fetch_one(&mut *txn)
    .await?;

    txn.commit().await?;

    Ok(record.id)
}

pub async fn read_default_image<'c, E>(executor: E) -> Result<Option<Image>, ImagesDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        select id, name, is_default
        from app.images
        where is_default = true
        "#,
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| Image {
        id: r.id,
        name: r.name,
        is_default: r.is_default,
    }))
}

pub async fn read_image<'c, E>(executor: E, image_id: i64) -> Result<Option<Image>, ImagesDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        select id, name, is_default
        from app.images
        where id = $1
        "#,
        image_id,
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| Image {
        id: r.id,
        name: r.name,
        is_default: r.is_default,
    }))
}

pub async fn update_image(
    pool: &PgPool,
    image_id: i64,
    name: &str,
    is_default: bool,
) -> Result<Option<i64>, ImagesDbError> {
    let mut txn = pool.begin().await?;

    // If this is to be the new default image, first unset any existing default
    if is_default {
        sqlx::query!(
            r#"
            update app.images
            set is_default = false
            where is_default = true and id != $1
            "#,
            image_id
        )
        .execute(&mut *txn)
        .await?;
    }

    let record = sqlx::query!(
        r#"
        update app.images
        set name = $1, is_default = $2
        where id = $3
        returning id
        "#,
        name,
        is_default,
        image_id
    )
    .fetch_optional(&mut *txn)
    .await?;

    txn.commit().await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_image(pool: &PgPool, image_id: i64) -> Result<Option<i64>, ImagesDbError> {
    let mut txn = pool.begin().await?;

    // First check if the image exists and if it's a default
    let image = sqlx::query!(
        r#"
        select id, is_default
        from app.images
        where id = $1
        "#,
        image_id
    )
    .fetch_optional(&mut *txn)
    .await?;

    let image = match image {
        Some(img) => img,
        None => {
            // Image doesn't exist, return None
            return Ok(None);
        }
    };

    // Prevent deletion if it's a default image
    if image.is_default {
        return Err(ImagesDbError::CannotDeleteDefault);
    }

    // Proceed with deletion
    let record = sqlx::query!(
        r#"
        delete from app.images
        where id = $1
        returning id
        "#,
        image_id
    )
    .fetch_optional(&mut *txn)
    .await?;

    txn.commit().await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_images<'c, E>(executor: E) -> Result<Vec<Image>, ImagesDbError>
where
    E: PgExecutor<'c>,
{
    let mut record = sqlx::query!(
        r#"
        select id, name, is_default
        from app.images
        "#,
    )
    .fetch_all(executor)
    .await?;

    Ok(record
        .drain(..)
        .map(|r| Image {
            id: r.id,
            name: r.name,
            is_default: r.is_default,
        })
        .collect())
}

pub async fn read_image_by_replicator_id<'c, E>(
    executor: E,
    replicator_id: i64,
) -> Result<Option<Image>, ImagesDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        select i.id, i.name, i.is_default
        from app.images i
        join app.replicators r on i.id = r.image_id
        where r.id = $1
        "#,
        replicator_id,
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| Image {
        id: r.id,
        name: r.name,
        is_default: r.is_default,
    }))
}
