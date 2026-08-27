use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

/// Source schemas hidden from replication table and schema discovery.
pub(crate) const EXCLUDED_SOURCE_SCHEMAS: &[&str] = &[
    "pg_catalog",
    "information_schema",
    "auth",
    "etl",
    "extensions",
    "graphql",
    "pgtle",
    "pgsodium",
    "realtime",
    "storage",
    "vault",
];

#[derive(Debug, Error)]
pub enum TablesDbError {
    /// The source database query failed.
    #[error("Error while interacting with Postgres for tables")]
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

/// Builds a [`SourceTable`] from PostgreSQL catalog values.
pub(crate) fn source_table_from_postgres(id: i64, schema: String, name: String) -> SourceTable {
    SourceTable { id: u32::try_from(id).expect("Postgres OIDs fit in u32"), schema, name }
}

/// Returns the ordinary and partitioned tables available for replication.
pub async fn get_tables(pool: &PgPool) -> Result<Vec<SourceTable>, TablesDbError> {
    get_tables_with_schema(pool, None).await
}

/// Returns replication tables, optionally restricted to one schema.
pub async fn get_tables_with_schema(
    pool: &PgPool,
    schema: Option<&str>,
) -> Result<Vec<SourceTable>, TablesDbError> {
    let query = r#"
        select
            c.oid::bigint as id,
            n.nspname as schema,
            c.relname as name
        from pg_catalog.pg_class c
        join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        where
            c.relkind in ('r', 'p')
            and c.relpersistence = 'p'
            and not (n.nspname = any($2::text[]))
            and n.nspname !~ '^pg_'
            and ($1::text is null or n.nspname = $1)
        order by n.nspname, c.relname;
        "#;

    let tables = sqlx::query_as::<_, (i64, String, String)>(query)
        .bind(schema)
        .bind(EXCLUDED_SOURCE_SCHEMAS)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|(id, schema, name)| source_table_from_postgres(id, schema, name))
        .collect();

    Ok(tables)
}
