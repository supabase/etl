use serde::{Deserialize, Serialize};
use sqlx::{Executor, PgPool, Row};
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Debug, Error)]
pub enum TablesDbError {
    #[error("Error while interacting with PostgreSQL for tables: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Table {
    pub schema: String,
    pub name: String,
}

pub async fn get_tables(pool: &PgPool) -> Result<Vec<Table>, TablesDbError> {
    let query = r#"
        select
           	n.nspname as schema,
           	c.relname as name
        from pg_catalog.pg_class c
           	left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            left join pg_catalog.pg_am am on am.oid = c.relam
        where
           	c.relkind = 'r'
           	and n.nspname <> 'pg_catalog'
            and n.nspname !~ '^pg_toast'
            and n.nspname <> 'information_schema'
           	and pg_catalog.pg_table_is_visible(c.oid)
        order by schema, name;
        "#;

    let tables = pool
        .fetch_all(query)
        .await?
        .iter()
        .map(|r| Table {
            schema: r.get("schema"),
            name: r.get("name"),
        })
        .collect();

    Ok(tables)
}
