use sqlx::postgres::PgRow;
use sqlx::postgres::types::Oid;
use sqlx::{PgExecutor, PgPool, Row};
use std::collections::HashMap;
use tokio_postgres::types::Type as PgType;

use crate::types::{ColumnSchema, TableId, TableName, TableSchema};

macro_rules! define_type_mappings {
    (
        $(
            $pg_type:ident => $string_name:literal
        ),* $(,)?
    ) => {
        /// Converts a Postgres type name string to a [`PgType`].
        ///
        /// Maps string representations to their corresponding Postgres types,
        /// handling common types and falling back to `TEXT` for unknown types.
        pub fn string_to_postgres_type(type_str: &str) -> PgType {
            match type_str {
                $(
                    $string_name => PgType::$pg_type,
                )*
                _ => PgType::TEXT, // Fallback for unknown types
            }
        }

        /// Converts a Postgres [`PgType`] to its string representation.
        ///
        /// Maps Postgres types to string equivalents for database storage,
        /// handling common types with fallback for unknown types.
        pub fn postgres_type_to_string(pg_type: &PgType) -> String {
            match *pg_type {
                $(
                    PgType::$pg_type => $string_name.to_string(),
                )*
                _ => format!("UNKNOWN({})", pg_type.name()),
            }
        }

        #[cfg(test)]
        pub fn get_test_type_mappings() -> Vec<(PgType, &'static str)> {
            vec![
                $(
                    (PgType::$pg_type, $string_name),
                )*
            ]
        }
    };
}

define_type_mappings! {
    // Basic types
    BOOL => "BOOL",
    CHAR => "CHAR",
    INT2 => "INT2",
    INT4 => "INT4",
    INT8 => "INT8",
    FLOAT4 => "FLOAT4",
    FLOAT8 => "FLOAT8",
    TEXT => "TEXT",
    VARCHAR => "VARCHAR",
    TIMESTAMP => "TIMESTAMP",
    TIMESTAMPTZ => "TIMESTAMPTZ",
    DATE => "DATE",
    TIME => "TIME",
    TIMETZ => "TIMETZ",
    BYTEA => "BYTEA",
    UUID => "UUID",
    JSON => "JSON",
    JSONB => "JSONB",

    // Character types
    NAME => "NAME",
    BPCHAR => "BPCHAR",

    // Numeric types
    NUMERIC => "NUMERIC",
    MONEY => "MONEY",

    // Date/time types
    INTERVAL => "INTERVAL",

    // Network types
    INET => "INET",
    CIDR => "CIDR",
    MACADDR => "MACADDR",
    MACADDR8 => "MACADDR8",

    // Bit string types
    BIT => "BIT",
    VARBIT => "VARBIT",

    // Geometric types
    POINT => "POINT",
    LSEG => "LSEG",
    PATH => "PATH",
    BOX => "BOX",
    POLYGON => "POLYGON",
    LINE => "LINE",
    CIRCLE => "CIRCLE",

    // Other common types
    OID => "OID",
    XML => "XML",

    // Text search types
    TS_VECTOR => "TSVECTOR",
    TSQUERY => "TSQUERY",

    // Array types
    BOOL_ARRAY => "BOOL_ARRAY",
    INT2_ARRAY => "INT2_ARRAY",
    INT4_ARRAY => "INT4_ARRAY",
    INT8_ARRAY => "INT8_ARRAY",
    FLOAT4_ARRAY => "FLOAT4_ARRAY",
    FLOAT8_ARRAY => "FLOAT8_ARRAY",
    TEXT_ARRAY => "TEXT_ARRAY",
    VARCHAR_ARRAY => "VARCHAR_ARRAY",
    TIMESTAMP_ARRAY => "TIMESTAMP_ARRAY",
    TIMESTAMPTZ_ARRAY => "TIMESTAMPTZ_ARRAY",
    DATE_ARRAY => "DATE_ARRAY",
    UUID_ARRAY => "UUID_ARRAY",
    JSON_ARRAY => "JSON_ARRAY",
    JSONB_ARRAY => "JSONB_ARRAY",
    NUMERIC_ARRAY => "NUMERIC_ARRAY",

    // Range types
    INT4_RANGE => "INT4_RANGE",
    INT8_RANGE => "INT8_RANGE",
    NUM_RANGE => "NUM_RANGE",
    TS_RANGE => "TS_RANGE",
    TSTZ_RANGE => "TSTZ_RANGE",
    DATE_RANGE => "DATE_RANGE"
}

/// Stores a table schema in the database.
///
/// Inserts or updates table schema and column information in schema storage tables
/// using a transaction to ensure atomicity.
pub async fn store_table_schema(
    pool: &PgPool,
    pipeline_id: i64,
    table_schema: &TableSchema,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Insert or update table schema record
    let table_schema_id: i64 = sqlx::query(
        r#"
        insert into etl.table_schemas (pipeline_id, table_id, schema_name, table_name)
        values ($1, $2, $3, $4)
        on conflict (pipeline_id, table_id)
        do update set 
            schema_name = excluded.schema_name,
            table_name = excluded.table_name,
            updated_at = now()
        returning id
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
    sqlx::query("delete from etl.table_columns where table_schema_id = $1")
        .bind(table_schema_id)
        .execute(&mut *tx)
        .await?;

    // Insert all columns
    for (column_order, column_schema) in table_schema.column_schemas.iter().enumerate() {
        let column_type_str = postgres_type_to_string(&column_schema.typ);

        sqlx::query(
            r#"
            insert into etl.table_columns 
            (table_schema_id, column_name, column_type, type_modifier, nullable, primary_key, column_order)
            values ($1, $2, $3, $4, $5, $6, $7)
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

/// Loads all table schemas for a pipeline from the database.
///
/// Retrieves table schemas and columns from schema storage tables,
/// reconstructing complete [`TableSchema`] objects.
pub async fn load_table_schemas(
    pool: &PgPool,
    pipeline_id: i64,
) -> Result<Vec<TableSchema>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        select
            ts.table_id,
            ts.schema_name,
            ts.table_name,
            tc.column_name,
            tc.column_type,
            tc.type_modifier,
            tc.nullable,
            tc.primary_key,
            tc.column_order
        from etl.table_schemas ts
        inner join etl.table_columns tc on ts.id = tc.table_schema_id
        where ts.pipeline_id = $1
        order by ts.table_id, tc.column_order
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

/// Deletes all table schemas for a pipeline from the database.
///
/// Removes all table schema records and associated columns for the specified
/// pipeline, using CASCADE delete for automatic cleanup of related column records.
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

/// Builds a [`ColumnSchema`] from a database row.
///
/// Assumes all required fields are present in the row after appropriate joins.
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
        let test_types = get_test_type_mappings();

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

        // Test array and range syntax specifically
        assert_eq!(string_to_postgres_type("INT4_ARRAY"), Type::INT4_ARRAY);
        assert_eq!(string_to_postgres_type("TEXT_ARRAY"), Type::TEXT_ARRAY);
        assert_eq!(postgres_type_to_string(&Type::BOOL_ARRAY), "BOOL_ARRAY");
        assert_eq!(postgres_type_to_string(&Type::UUID_ARRAY), "UUID_ARRAY");
        assert_eq!(string_to_postgres_type("INT4_RANGE"), Type::INT4_RANGE);
        assert_eq!(postgres_type_to_string(&Type::DATE_RANGE), "DATE_RANGE");
    }
}
