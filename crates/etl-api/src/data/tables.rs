use serde::{Deserialize, Serialize};
use sqlx::{Executor, PgPool, Row};
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Debug, Error)]
pub enum TablesDbError {
    #[error("Error while interacting with Postgres for tables: {0}")]
    Database(#[from] sqlx::Error),
}

/// A schema-qualified table supplied in publication write requests.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Table {
    /// The Postgres schema containing the table.
    pub schema: String,
    /// The unqualified Postgres table name.
    pub name: String,
}

/// A table discovered in a source database.
///
/// The `id` is the table's Postgres OID. It is stable across renames for the
/// lifetime of the relation, but it is scoped to this source database and may
/// change if the table is dropped and recreated. Callers should use `schema`
/// and `name` for display. For selective table-copy configuration, use the
/// IDs returned by the publication response: the generic source-tables
/// response contains both partition roots and leaves, while the publication
/// response follows its `publish_via_partition_root` setting.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SourceTable {
    /// The table's Postgres OID in this source database.
    pub id: u32,
    /// The Postgres schema containing the table.
    pub schema: String,
    /// The unqualified Postgres table name.
    pub name: String,
}

/// Returns the ordinary and partitioned tables available for replication.
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
            c.relkind in ('r', 'p')
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
