use sqlx::{PgPool, Postgres, Transaction};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TenantsDbError {
    #[error("Error while interacting with PostgreSQL for tenants: {0}")]
    Database(#[from] sqlx::Error),
}

pub struct Tenant {
    pub id: String,
    pub name: String,
}

pub async fn create_tenant(
    pool: &PgPool,
    tenant_id: &str,
    tenant_name: &str,
) -> Result<String, TenantsDbError> {
    let mut txn = pool.begin().await?;
    let res = create_tenant_txn(&mut txn, tenant_id, tenant_name).await;
    txn.commit().await?;
    res
}

pub async fn create_tenant_txn(
    txn: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    tenant_name: &str,
) -> Result<String, TenantsDbError> {
    let record = sqlx::query!(
        r#"
        insert into app.tenants (id, name)
        values ($1, $2)
        returning id
        "#,
        tenant_id,
        tenant_name,
    )
    .fetch_one(&mut **txn)
    .await?;

    Ok(record.id)
}

pub async fn create_or_update_tenant(
    pool: &PgPool,
    tenant_id: &str,
    tenant_name: &str,
) -> Result<String, TenantsDbError> {
    let record = sqlx::query!(
        r#"
        insert into app.tenants (id, name)
        values ($1, $2)
        on conflict (id) do update set name = $2
        returning id
        "#,
        tenant_id,
        tenant_name,
    )
    .fetch_one(pool)
    .await?;

    Ok(record.id)
}

pub async fn read_tenant(pool: &PgPool, tenant_id: &str) -> Result<Option<Tenant>, TenantsDbError> {
    let record = sqlx::query!(
        r#"
        select id, name
        from app.tenants
        where id = $1
        "#,
        tenant_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| Tenant {
        id: r.id,
        name: r.name,
    }))
}

pub async fn update_tenant(
    pool: &PgPool,
    tenant_id: &str,
    tenant_name: &str,
) -> Result<Option<String>, TenantsDbError> {
    let record = sqlx::query!(
        r#"
        update app.tenants
        set name = $1
        where id = $2
        returning id
        "#,
        tenant_name,
        tenant_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_tenant(
    pool: &PgPool,
    tenant_id: &str,
) -> Result<Option<String>, TenantsDbError> {
    let record = sqlx::query!(
        r#"
        delete from app.tenants
        where id = $1
        returning id
        "#,
        tenant_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_tenants(pool: &PgPool) -> Result<Vec<Tenant>, TenantsDbError> {
    let mut record = sqlx::query!(
        r#"
        select id, name
        from app.tenants
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(record
        .drain(..)
        .map(|r| Tenant {
            id: r.id,
            name: r.name,
        })
        .collect())
}
