//! V2 source-schema data access.

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use crate::data::tables::EXCLUDED_SOURCE_SCHEMAS;

/// An error returned while reading schemas from a source database.
#[derive(Debug, Error)]
pub enum SchemasDbError {
    /// The source database query failed.
    #[error("Error while interacting with Postgres for schemas")]
    Database(#[from] sqlx::Error),
}

/// A schema discovered in a source database.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SourceSchema {
    /// The schema name.
    pub name: String,
}

/// Returns the source schemas available for replication.
pub async fn get_schemas(pool: &PgPool) -> Result<Vec<SourceSchema>, SchemasDbError> {
    let query = r#"
        select n.nspname as name
        from pg_catalog.pg_namespace n
        where not (n.nspname = any($1::text[]))
            and n.nspname !~ '^pg_'
        order by n.nspname;
        "#;

    let schemas = sqlx::query_scalar::<_, String>(query)
        .bind(EXCLUDED_SOURCE_SCHEMAS)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|name| SourceSchema { name })
        .collect();

    Ok(schemas)
}
