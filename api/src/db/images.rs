use sqlx::{PgPool, Postgres, Transaction};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ImagesDbError {
    #[error("Error while interacting with PostgreSQL for images: {0}")]
    Database(#[from] sqlx::Error),
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
    let res = create_image_txn(&mut txn, name, is_default).await;
    txn.commit().await?;
    res
}

pub async fn create_image_txn(
    txn: &mut Transaction<'_, Postgres>,
    name: &str,
    is_default: bool,
) -> Result<i64, ImagesDbError> {
    let record = sqlx::query!(
        r#"
        insert into app.images (name, is_default)
        values ($1, $2)
        returning id
        "#,
        name,
        is_default
    )
    .fetch_one(&mut **txn)
    .await?;

    Ok(record.id)
}

pub async fn read_default_image(pool: &PgPool) -> Result<Option<Image>, ImagesDbError> {
    let record = sqlx::query!(
        r#"
        select id, name, is_default
        from app.images
        where is_default = true
        "#,
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| Image {
        id: r.id,
        name: r.name,
        is_default: r.is_default,
    }))
}

pub async fn read_image(pool: &PgPool, image_id: i64) -> Result<Option<Image>, ImagesDbError> {
    let record = sqlx::query!(
        r#"
        select id, name, is_default
        from app.images
        where id = $1
        "#,
        image_id,
    )
    .fetch_optional(pool)
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
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_image(pool: &PgPool, image_id: i64) -> Result<Option<i64>, ImagesDbError> {
    let record = sqlx::query!(
        r#"
        delete from app.images
        where id = $1
        returning id
        "#,
        image_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_images(pool: &PgPool) -> Result<Vec<Image>, ImagesDbError> {
    let mut record = sqlx::query!(
        r#"
        select id, name, is_default
        from app.images
        "#,
    )
    .fetch_all(pool)
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

pub async fn read_image_by_replicator_id(
    pool: &PgPool,
    replicator_id: i64,
) -> Result<Option<Image>, ImagesDbError> {
    let record = sqlx::query!(
        r#"
        select i.id, i.name, i.is_default
        from app.images i
        join app.replicators r on i.id = r.image_id
        where r.id = $1
        "#,
        replicator_id,
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| Image {
        id: r.id,
        name: r.name,
        is_default: r.is_default,
    }))
}
