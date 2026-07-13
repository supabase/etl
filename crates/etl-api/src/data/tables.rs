use serde::{Deserialize, Serialize};
use sqlx::{Executor, PgPool, Row};
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Debug, Error)]
pub enum TablesDbError {
    #[error("Error while interacting with Postgres for tables: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Table {
    pub schema: String,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SourceTable {
    pub id: u32,
    pub schema: String,
    pub name: String,
}

/// A table referenced by a pipeline's table-sync-copy configuration.
///
/// `schema` and `name` are `None` when the table id no longer resolves to a
/// table in the source database, for example because the table was dropped
/// after being selected. Callers can render the table as missing directly
/// from this shape instead of treating an unresolved id as an error.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ConfiguredTable {
    pub id: u32,
    pub schema: Option<String>,
    pub name: Option<String>,
}

pub async fn get_tables(pool: &PgPool) -> Result<Vec<SourceTable>, TablesDbError> {
    let query = r#"
        select
			c.oid::bigint as id,
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
        "#;

    let tables = pool
        .fetch_all(query)
        .await?
        .iter()
        .map(|r| SourceTable {
            id: u32::try_from(r.get::<i64, _>("id")).expect("Postgres OIDs fit in u32"),
            schema: r.get("schema"),
            name: r.get("name"),
        })
        .collect();

    Ok(tables)
}
