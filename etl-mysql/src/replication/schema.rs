/// MySQL schema information utilities.
///
/// This module provides functions for querying MySQL's information_schema
/// to retrieve table and column metadata.

use sqlx::{MySqlPool, Row};
use thiserror::Error;

use crate::types::{ColumnSchema, TableId, TableName, TableSchema};

/// Errors that can occur during schema operations.
#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Table {0} not found")]
    TableNotFound(String),

    #[error("Invalid schema data: {0}")]
    InvalidData(String),
}

/// Retrieves the complete schema for a table.
pub async fn get_table_schema(
    pool: &MySqlPool,
    table_name: &TableName,
) -> Result<TableSchema, SchemaError> {
    let columns = get_table_columns(pool, table_name).await?;

    if columns.is_empty() {
        return Err(SchemaError::TableNotFound(table_name.to_string()));
    }

    let table_id = compute_table_id(table_name);

    Ok(TableSchema::new(table_id, table_name.clone(), columns))
}

/// Retrieves column information for a table.
async fn get_table_columns(
    pool: &MySqlPool,
    table_name: &TableName,
) -> Result<Vec<ColumnSchema>, SchemaError> {
    let query = r#"
        SELECT 
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.is_nullable,
            CASE WHEN k.column_name IS NOT NULL THEN 1 ELSE 0 END as is_primary
        FROM information_schema.columns c
        LEFT JOIN information_schema.key_column_usage k
            ON c.table_schema = k.table_schema
            AND c.table_name = k.table_name
            AND c.column_name = k.column_name
            AND k.constraint_name = 'PRIMARY'
        WHERE c.table_schema = ?
        AND c.table_name = ?
        ORDER BY c.ordinal_position
    "#;

    let rows = sqlx::query(query)
        .bind(&table_name.schema)
        .bind(&table_name.name)
        .fetch_all(pool)
        .await?;

    let mut columns = Vec::new();
    for row in rows {
        let column_name: String = row.try_get("column_name")?;
        let data_type: String = row.try_get("data_type")?;
        let max_length: Option<i64> = row.try_get("character_maximum_length")?;
        let is_nullable: String = row.try_get("is_nullable")?;
        let is_primary: i32 = row.try_get("is_primary")?;

        let modifier = max_length.unwrap_or(-1) as i32;
        let nullable = is_nullable == "YES";
        let primary = is_primary == 1;

        columns.push(ColumnSchema::new(
            column_name,
            data_type.to_uppercase(),
            modifier,
            nullable,
            primary,
        ));
    }

    Ok(columns)
}

/// Computes a deterministic table ID from the table name.
///
/// Since MySQL doesn't have a direct equivalent to PostgreSQL's OID,
/// we generate a hash-based ID from the table's fully qualified name.
pub fn compute_table_id(table_name: &TableName) -> TableId {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    table_name.schema.hash(&mut hasher);
    table_name.name.hash(&mut hasher);
    let hash = hasher.finish();

    TableId::new((hash & 0xFFFFFFFF) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_table_id_deterministic() {
        let table1 = TableName::new("test_db".to_string(), "users".to_string());
        let table2 = TableName::new("test_db".to_string(), "users".to_string());
        let table3 = TableName::new("test_db".to_string(), "posts".to_string());

        let id1 = compute_table_id(&table1);
        let id2 = compute_table_id(&table2);
        let id3 = compute_table_id(&table3);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }
}
