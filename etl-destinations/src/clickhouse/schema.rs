use etl::types::{ColumnSchema, Type, is_array_type};

/// Returns the base ClickHouse type string for a Postgres scalar type.
///
/// The returned string does not include `Nullable(...)` wrapping — callers are
/// responsible for applying that when the column is nullable. Arrays always use
/// `Array(Nullable(T))` since Postgres array elements are nullable.
pub fn postgres_column_type_to_clickhouse_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::BOOL => "Boolean",
        &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => "String",
        &Type::INT2 => "Int16",
        &Type::INT4 => "Int32",
        &Type::INT8 => "Int64",
        &Type::FLOAT4 => "Float32",
        &Type::FLOAT8 => "Float64",
        &Type::NUMERIC => "String",
        &Type::DATE => "Date",
        &Type::TIME => "String",
        &Type::TIMESTAMP => "DateTime64(6)",
        &Type::TIMESTAMPTZ => "DateTime64(6, 'UTC')",
        &Type::UUID => "UUID",
        &Type::JSON | &Type::JSONB => "String",
        &Type::BYTEA => "String",
        &Type::OID => "UInt32",
        _ => "String",
    }
}

/// Returns the ClickHouse array element type for a Postgres array type.
///
/// Used by [`build_create_table_sql`] to construct `Array(Nullable(T))` columns.
fn postgres_array_element_clickhouse_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::BOOL_ARRAY => "Boolean",
        &Type::CHAR_ARRAY
        | &Type::BPCHAR_ARRAY
        | &Type::VARCHAR_ARRAY
        | &Type::NAME_ARRAY
        | &Type::TEXT_ARRAY => "String",
        &Type::INT2_ARRAY => "Int16",
        &Type::INT4_ARRAY => "Int32",
        &Type::INT8_ARRAY => "Int64",
        &Type::FLOAT4_ARRAY => "Float32",
        &Type::FLOAT8_ARRAY => "Float64",
        &Type::NUMERIC_ARRAY => "String",
        &Type::DATE_ARRAY => "Date",
        &Type::TIME_ARRAY => "String",
        &Type::TIMESTAMP_ARRAY => "DateTime64(6)",
        &Type::TIMESTAMPTZ_ARRAY => "DateTime64(6, 'UTC')",
        &Type::UUID_ARRAY => "UUID",
        &Type::JSON_ARRAY | &Type::JSONB_ARRAY => "String",
        &Type::BYTEA_ARRAY => "String",
        &Type::OID_ARRAY => "UInt32",
        _ => "String",
    }
}

/// Converts a Postgres `public.my_table` style table name into a ClickHouse table
/// name using the same double-underscore escaping convention used by DuckLake/Iceberg.
///
/// - Schema and table are joined with `_`
/// - Any literal `_` in the schema or table name is escaped to `__`
///
/// Examples:
/// - `public.orders`  → `public_orders`
/// - `my_schema.t`    → `my__schema_t`
pub fn table_name_to_clickhouse_table_name(schema: &str, table: &str) -> String {
    let escaped_schema = schema.replace('_', "__");
    let escaped_table = table.replace('_', "__");
    format!("{escaped_schema}_{escaped_table}")
}

/// Generates a `CREATE TABLE IF NOT EXISTS` SQL statement for the given columns.
///
/// - Non-nullable columns use the bare ClickHouse type (`Int32`, `String`, …).
/// - Nullable columns use `Nullable(T)`.
/// - Array columns always use `Array(Nullable(T))` (Postgres array elements are nullable).
/// - Two CDC trailing columns are always appended as non-nullable:
///   `cdc_operation String, cdc_lsn Int64`
/// - The table uses `MergeTree()` with `ORDER BY tuple()` (pure append order).
pub fn build_create_table_sql(table_name: &str, column_schemas: &[ColumnSchema]) -> String {
    let mut cols = Vec::with_capacity(column_schemas.len() + 2);

    for col in column_schemas {
        let col_type = if is_array_type(&col.typ) {
            let elem = postgres_array_element_clickhouse_sql(&col.typ);
            format!("Array(Nullable({elem}))")
        } else {
            let base = postgres_column_type_to_clickhouse_sql(&col.typ);
            if col.nullable {
                format!("Nullable({base})")
            } else {
                base.to_string()
            }
        };
        cols.push(format!("  \"{}\" {}", col.name, col_type));
    }

    // CDC columns — always non-nullable
    cols.push("  \"cdc_operation\" String".to_string());
    cols.push("  \"cdc_lsn\" Int64".to_string());

    let col_defs = cols.join(",\n");
    format!(
        "CREATE TABLE IF NOT EXISTS \"{table_name}\" (\n{col_defs}\n) ENGINE = MergeTree()\nORDER BY tuple()"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_escaping() {
        assert_eq!(
            table_name_to_clickhouse_table_name("public", "orders"),
            "public_orders"
        );
        assert_eq!(
            table_name_to_clickhouse_table_name("my_schema", "my_table"),
            "my__schema_my__table"
        );
        assert_eq!(
            table_name_to_clickhouse_table_name("public", "my__table"),
            "public_my____table"
        );
    }

    #[test]
    fn test_scalar_type_mapping() {
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::BOOL), "Boolean");
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::CHAR),
            "String"
        );
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::BPCHAR),
            "String"
        );
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::VARCHAR),
            "String"
        );
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::NAME),
            "String"
        );
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::TEXT),
            "String"
        );
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::INT2), "Int16");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::INT4), "Int32");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::INT8), "Int64");
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::FLOAT4),
            "Float32"
        );
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::FLOAT8),
            "Float64"
        );
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::NUMERIC),
            "String"
        );
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::DATE), "Date");
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::TIME),
            "String"
        );
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::TIMESTAMP),
            "DateTime64(6)"
        );
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::TIMESTAMPTZ),
            "DateTime64(6, 'UTC')"
        );
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::UUID), "UUID");
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::JSON),
            "String"
        );
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::JSONB),
            "String"
        );
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::BYTEA),
            "String"
        );
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::OID), "UInt32");
    }

    #[test]
    fn test_array_type_mapping() {
        assert_eq!(
            postgres_array_element_clickhouse_sql(&Type::BOOL_ARRAY),
            "Boolean"
        );
        assert_eq!(
            postgres_array_element_clickhouse_sql(&Type::TEXT_ARRAY),
            "String"
        );
        assert_eq!(
            postgres_array_element_clickhouse_sql(&Type::INT4_ARRAY),
            "Int32"
        );
        assert_eq!(
            postgres_array_element_clickhouse_sql(&Type::INT8_ARRAY),
            "Int64"
        );
        assert_eq!(
            postgres_array_element_clickhouse_sql(&Type::FLOAT8_ARRAY),
            "Float64"
        );
        assert_eq!(
            postgres_array_element_clickhouse_sql(&Type::UUID_ARRAY),
            "UUID"
        );
        assert_eq!(
            postgres_array_element_clickhouse_sql(&Type::JSONB_ARRAY),
            "String"
        );
    }

    #[test]
    fn test_build_create_table_sql_nullable() {
        let schemas = vec![
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::INT4,
                modifier: -1,
                nullable: false,
                primary: true,
            },
            ColumnSchema {
                name: "name".to_string(),
                typ: Type::TEXT,
                modifier: -1,
                nullable: true,
                primary: false,
            },
        ];
        let sql = build_create_table_sql("public_users", &schemas);
        assert!(sql.contains("\"id\" Int32"), "id should be non-nullable Int32");
        assert!(
            sql.contains("\"name\" Nullable(String)"),
            "name should be Nullable(String)"
        );
    }

    #[test]
    fn test_build_create_table_sql_cdc_columns() {
        let schemas = vec![ColumnSchema {
            name: "id".to_string(),
            typ: Type::INT4,
            modifier: -1,
            nullable: false,
            primary: true,
        }];
        let sql = build_create_table_sql("public_t", &schemas);
        assert!(
            sql.contains("\"cdc_operation\" String"),
            "cdc_operation should be non-nullable"
        );
        assert!(
            sql.contains("\"cdc_lsn\" Int64"),
            "cdc_lsn should be non-nullable Int64"
        );
        assert!(sql.contains("ENGINE = MergeTree()"));
        assert!(sql.contains("ORDER BY tuple()"));
    }

    #[test]
    fn test_build_create_table_sql_array_columns() {
        let schemas = vec![ColumnSchema {
            name: "tags".to_string(),
            typ: Type::TEXT_ARRAY,
            modifier: -1,
            nullable: false,
            primary: false,
        }];
        let sql = build_create_table_sql("public_t", &schemas);
        assert!(
            sql.contains("\"tags\" Array(Nullable(String))"),
            "array columns should always be Array(Nullable(T))"
        );
    }
}
