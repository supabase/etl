use std::collections::HashMap;

use thiserror::Error;

pub use crate::db::extract_server_version;
use crate::{
    tokio::PgSourceTransaction,
    types::{TableId, TableName},
};

/// Errors that can occur during table lookups.
#[derive(Debug, Error)]
pub enum TableLookupError {
    /// A database operation failed.
    #[error("Database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    /// A table ID was not present in Postgres catalogs.
    #[error("Table with ID {0} not found")]
    TableNotFound(TableId),
}

/// Reads source table names for table IDs.
pub async fn table_names_from_table_ids(
    txn: &PgSourceTransaction<'_>,
    table_ids: &[TableId],
) -> Result<HashMap<TableId, TableName>, TableLookupError> {
    if table_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let ids = table_ids.to_vec();
    let rows = txn
        .query(
            r#"
            select c.oid as oid, n.nspname as schema_name, c.relname as table_name
            from pg_class c
            join pg_namespace n on c.relnamespace = n.oid
            where c.oid = any($1::oid[])
            "#,
            &[&ids],
        )
        .await?;

    let mut result = HashMap::with_capacity(rows.len());
    for row in rows {
        let table_id = row.get("oid");
        let schema_name: String = row.get("schema_name");
        let table_name: String = row.get("table_name");
        result.insert(table_id, TableName { schema: schema_name, name: table_name });
    }

    Ok(result)
}
