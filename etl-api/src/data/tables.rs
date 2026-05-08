use etl_postgres::tokio::{PgSourceClient, PgSourceError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Debug, Error)]
pub enum TablesDbError {
    #[error("Error while interacting with Postgres for tables: {0}")]
    Database(#[from] PgSourceError),
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Table {
    pub schema: String,
    pub name: String,
}

pub async fn get_tables(source_client: &PgSourceClient) -> Result<Vec<Table>, TablesDbError> {
    let rows = source_client
        .query(
            r#"
            select
                n.nspname as schema,
                c.relname as name
            from pg_catalog.pg_class c
                left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                left join pg_catalog.pg_am am on am.oid = c.relam
            where
                c.relkind = 'r'
                and n.nspname not in ('pg_catalog', 'information_schema', 'auth', 'etl', 'extensions', 'graphql', 'pgtle', 'pgsodium', 'realtime', 'storage', 'vault')
                and n.nspname !~ '^pg_toast'
            order by schema, name;
            "#,
            &[],
        )
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| Table { schema: row.get("schema"), name: row.get("name") })
        .collect())
}
