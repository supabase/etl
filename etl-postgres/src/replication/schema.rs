use sqlx::postgres::PgRow;
use sqlx::postgres::types::Oid;
use sqlx::{PgExecutor, PgPool, Row};
use std::collections::HashMap;
use tokio_postgres::types::Type as PgType;

use crate::schema::{ColumnSchema, TableId, TableName, TableSchema};

/// Converts a PostgreSQL type name string back to a [`PgType`].
///
/// This function maps string representations back to their corresponding PostgreSQL types.
/// It handles the most common PostgreSQL types and falls back to `TEXT` for unknown types.
pub fn string_to_postgres_type(type_str: &str) -> PgType {
    match type_str {
        "BOOL" => PgType::BOOL,
        "CHAR" => PgType::CHAR,
        "INT2" => PgType::INT2,
        "INT4" => PgType::INT4,
        "INT8" => PgType::INT8,
        "FLOAT4" => PgType::FLOAT4,
        "FLOAT8" => PgType::FLOAT8,
        "TEXT" => PgType::TEXT,
        "VARCHAR" => PgType::VARCHAR,
        "TIMESTAMP" => PgType::TIMESTAMP,
        "TIMESTAMPTZ" => PgType::TIMESTAMPTZ,
        "DATE" => PgType::DATE,
        "TIME" => PgType::TIME,
        "TIMETZ" => PgType::TIMETZ,
        "BYTEA" => PgType::BYTEA,
        "UUID" => PgType::UUID,
        "JSON" => PgType::JSON,
        "JSONB" => PgType::JSONB,
        _ => PgType::TEXT, // Fallback for unknown types
    }
}

/// Converts a PostgreSQL [`PgType`] to its string representation.
///
/// This function maps PostgreSQL types to their string equivalents for storage in the database.
/// It handles the most common PostgreSQL types and provides a fallback for unknown types.
pub fn postgres_type_to_string(pg_type: &PgType) -> String {
    match *pg_type {
        PgType::BOOL => "BOOL".to_string(),
        PgType::CHAR => "CHAR".to_string(),
        PgType::INT2 => "INT2".to_string(),
        PgType::INT4 => "INT4".to_string(),
        PgType::INT8 => "INT8".to_string(),
        PgType::FLOAT4 => "FLOAT4".to_string(),
        PgType::FLOAT8 => "FLOAT8".to_string(),
        PgType::TEXT => "TEXT".to_string(),
        PgType::VARCHAR => "VARCHAR".to_string(),
        PgType::TIMESTAMP => "TIMESTAMP".to_string(),
        PgType::TIMESTAMPTZ => "TIMESTAMPTZ".to_string(),
        PgType::DATE => "DATE".to_string(),
        PgType::TIME => "TIME".to_string(),
        PgType::TIMETZ => "TIMETZ".to_string(),
        PgType::BYTEA => "BYTEA".to_string(),
        PgType::UUID => "UUID".to_string(),
        PgType::JSON => "JSON".to_string(),
        PgType::JSONB => "JSONB".to_string(),
        _ => format!("UNKNOWN({})", pg_type.name()),
    }
}

/// Stores a table schema in the database.
///
/// This function inserts or updates a table schema and its columns in the schema storage tables.
/// It uses a transaction to ensure atomicity.
pub async fn store_table_schema(
    pool: &PgPool,
    pipeline_id: i64,
    table_schema: &TableSchema,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Insert or update table schema record
    let table_schema_id: i64 = sqlx::query(
        r#"
        INSERT INTO etl.table_schemas (pipeline_id, table_id, schema_name, table_name)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (pipeline_id, table_id)
        DO UPDATE SET 
            schema_name = EXCLUDED.schema_name,
            table_name = EXCLUDED.table_name,
            updated_at = now()
        RETURNING id
        "#,
    )
    .bind(pipeline_id)
    .bind(table_schema.id.into_inner() as i64)
    .bind(&table_schema.name.schema)
    .bind(&table_schema.name.name)
    .fetch_one(&mut *tx)
    .await?
    .get(0);

    // Delete existing columns for this table schema to handle schema changes
    sqlx::query("DELETE FROM etl.table_columns WHERE table_schema_id = $1")
        .bind(table_schema_id)
        .execute(&mut *tx)
        .await?;

    // Insert all columns
    for (column_order, column_schema) in table_schema.column_schemas.iter().enumerate() {
        let column_type_str = postgres_type_to_string(&column_schema.typ);

        sqlx::query(
            r#"
            INSERT INTO etl.table_columns 
            (table_schema_id, column_name, column_type, type_modifier, nullable, primary_key, column_order)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(table_schema_id)
        .bind(&column_schema.name)
        .bind(column_type_str)
        .bind(column_schema.modifier)
        .bind(column_schema.nullable)
        .bind(column_schema.primary)
        .bind(column_order as i32)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;

    Ok(())
}

/// Loads all table schemas for a given pipeline from the database.
///
/// This function retrieves table schemas and their columns from the schema storage tables,
/// reconstructing [`TableSchema`] objects.
/// Loads all table schemas for a given pipeline from the database.
///
/// This function retrieves table schemas and their columns from the schema storage tables,
/// reconstructing [`TableSchema`] objects.
pub async fn load_table_schemas(
    pool: &PgPool,
    pipeline_id: i64,
) -> Result<Vec<TableSchema>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT
            ts.table_id,
            ts.schema_name,
            ts.table_name,
            tc.column_name,
            tc.column_type,
            tc.type_modifier,
            tc.nullable,
            tc.primary_key,
            tc.column_order
        FROM etl.table_schemas ts
        INNER JOIN etl.table_columns tc ON ts.id = tc.table_schema_id
        WHERE ts.pipeline_id = $1
        ORDER BY ts.table_id, tc.column_order
        "#,
    )
    .bind(pipeline_id)
    .fetch_all(pool)
    .await?;

    let mut table_schemas = HashMap::new();

    for row in rows {
        let table_oid: Oid = row.get("table_id");
        let table_id = TableId::new(table_oid.0);
        let schema_name: String = row.get("schema_name");
        let table_name: String = row.get("table_name");

        let entry = table_schemas.entry(table_id).or_insert_with(|| {
            TableSchema::new(table_id, TableName::new(schema_name, table_name), vec![])
        });

        entry.add_column_schema(parse_column_schema(&row));
    }

    Ok(table_schemas.into_values().collect())
}

/// Deletes all table schemas for a given pipeline from the database.
///
/// This function removes all table schema records and their associated columns
/// for a specific pipeline. Uses CASCADE delete from the foreign key constraint
/// to automatically remove related column records.
pub async fn delete_pipeline_table_schemas<'c, E>(
    executor: E,
    pipeline_id: i64,
) -> Result<u64, sqlx::Error>
where
    E: PgExecutor<'c>,
{
    let result = sqlx::query(
        r#"
        delete from etl.table_schemas
        where pipeline_id = $1
        "#,
    )
    .bind(pipeline_id)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

/// Builds a `ColumnSchema` from a database row.
///
/// Assumes all required fields are present (e.g. after INNER JOIN).
fn parse_column_schema(row: &PgRow) -> ColumnSchema {
    let column_name: String = row.get("column_name");
    let column_type: String = row.get("column_type");
    let type_modifier: i32 = row.get("type_modifier");
    let nullable: bool = row.get("nullable");
    let primary_key: bool = row.get("primary_key");

    ColumnSchema::new(
        column_name,
        string_to_postgres_type(&column_type),
        type_modifier,
        nullable,
        primary_key,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::types::Type;

    #[test]
    fn test_type_string_conversion() {
        let test_types = vec![
            (Type::BOOL, "BOOL"),
            (Type::CHAR, "CHAR"),
            (Type::INT2, "INT2"),
            (Type::INT4, "INT4"),
            (Type::INT8, "INT8"),
            (Type::FLOAT4, "FLOAT4"),
            (Type::FLOAT8, "FLOAT8"),
            (Type::TEXT, "TEXT"),
            (Type::VARCHAR, "VARCHAR"),
            (Type::TIMESTAMP, "TIMESTAMP"),
            (Type::TIMESTAMPTZ, "TIMESTAMPTZ"),
            (Type::DATE, "DATE"),
            (Type::TIME, "TIME"),
            (Type::TIMETZ, "TIMETZ"),
            (Type::BYTEA, "BYTEA"),
            (Type::UUID, "UUID"),
            (Type::JSON, "JSON"),
            (Type::JSONB, "JSONB"),
        ];

        // Test type to string conversion
        for (pg_type, expected_string) in &test_types {
            let result = postgres_type_to_string(pg_type);
            assert_eq!(result, *expected_string, "Failed for type: {pg_type:?}");
        }

        // Test string to type conversion
        for (expected_type, type_string) in &test_types {
            let result = string_to_postgres_type(type_string);
            assert_eq!(result, *expected_type, "Failed for string: {type_string}");
        }

        // Test roundtrip conversion
        for (original_type, _) in &test_types {
            let type_string = postgres_type_to_string(original_type);
            let converted_back = string_to_postgres_type(&type_string);
            assert_eq!(
                converted_back, *original_type,
                "Roundtrip failed for: {original_type:?}"
            );
        }

        // Test unknown type fallback
        assert_eq!(string_to_postgres_type("UNKNOWN_TYPE"), Type::TEXT);
    }
}
