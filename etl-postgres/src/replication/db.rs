use etl_config::shared::{IntoConnectOptions, PgConnectionConfig};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use thiserror::Error;

use crate::schema::{TableId, TableName};

#[derive(Debug, Error)]
pub enum TableLookupError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Table with ID {0} not found")]
    TableNotFound(TableId),
}

/// Connect to the source database with a minimal connection pool
#[cfg(feature = "replication")]
pub async fn connect_to_source_database(
    config: &PgConnectionConfig,
    min_connections: u32,
    max_connections: u32,
) -> Result<PgPool, sqlx::Error> {
    let options = config.with_db();

    let pool = PgPoolOptions::new()
        .min_connections(min_connections)
        .max_connections(max_connections)
        .connect_with(options)
        .await?;

    Ok(pool)
}

/// Loads the table name from a table OID by querying the source database.
///
/// This function connects to the source database and looks up the schema and table name
/// for the given table OID using the pg_class and pg_namespace system tables.
pub async fn get_table_name_from_oid(
    pool: &PgPool,
    table_id: TableId,
) -> Result<TableName, TableLookupError> {
    let query = "
        select n.nspname as schema_name, c.relname as table_name
        from pg_class c
        join pg_namespace n ON c.relnamespace = n.oid
        where c.oid = $1
    ";

    let row = sqlx::query(query)
        .bind(table_id.into_inner() as i64)
        .fetch_optional(pool)
        .await?;

    match row {
        Some(row) => {
            let schema_name: String = row.try_get("schema_name")?;
            let table_name: String = row.try_get("table_name")?;

            Ok(TableName {
                schema: schema_name,
                name: table_name,
            })
        }
        None => Err(TableLookupError::TableNotFound(table_id)),
    }
}
