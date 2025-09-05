use sqlx::PgExecutor;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TenantsDbError {
    #[error("Error while interacting with Postgres for tenants: {0}")]
    Database(#[from] sqlx::Error),
}

pub struct Tenant {
    pub id: String,
    pub name: String,
}

pub async fn create_tenant<'c, E>(
    executor: E,
    tenant_id: &str,
    tenant_name: &str,
) -> Result<String, TenantsDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        insert into app.tenants (id, name)
        values ($1, $2)
        returning id
        "#,
        tenant_id,
        tenant_name,
    )
    .fetch_one(executor)
    .await?;

    Ok(record.id)
}

pub async fn create_or_update_tenant<'c, E>(
    executor: E,
    tenant_id: &str,
    tenant_name: &str,
) -> Result<String, TenantsDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        insert into app.tenants (id, name)
        values ($1, $2)
        on conflict (id) do update set name = $2, updated_at = now()
        returning id
        "#,
        tenant_id,
        tenant_name,
    )
    .fetch_one(executor)
    .await?;

    Ok(record.id)
}

pub async fn read_tenant<'c, E>(
    executor: E,
    tenant_id: &str,
) -> Result<Option<Tenant>, TenantsDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        select id, name
        from app.tenants
        where id = $1
        "#,
        tenant_id
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| Tenant {
        id: r.id,
        name: r.name,
    }))
}

pub async fn update_tenant<'c, E>(
    executor: E,
    tenant_id: &str,
    tenant_name: &str,
) -> Result<Option<String>, TenantsDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        update app.tenants
        set name = $1, updated_at = now()
        where id = $2
        returning id
        "#,
        tenant_name,
        tenant_id
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_tenant<'c, E>(
    executor: E,
    tenant_id: &str,
) -> Result<Option<String>, TenantsDbError>
where
    E: PgExecutor<'c>,
{
    let record = sqlx::query!(
        r#"
        delete from app.tenants
        where id = $1
        returning id
        "#,
        tenant_id
    )
    .fetch_optional(executor)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_tenants<'c, E>(executor: E) -> Result<Vec<Tenant>, TenantsDbError>
where
    E: PgExecutor<'c>,
{
    let mut record = sqlx::query!(
        r#"
        select id, name
        from app.tenants
        "#,
    )
    .fetch_all(executor)
    .await?;

    Ok(record
        .drain(..)
        .map(|r| Tenant {
            id: r.id,
            name: r.name,
        })
        .collect())
}
