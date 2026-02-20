use etl::types::{ColumnSchema, Type, is_array_type};

/// Returns the DuckDB SQL type string for a given Postgres scalar type.
fn postgres_scalar_type_to_duckdb_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::BOOL => "BOOLEAN",
        &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => "VARCHAR",
        &Type::INT2 => "SMALLINT",
        &Type::INT4 => "INTEGER",
        &Type::INT8 => "BIGINT",
        &Type::FLOAT4 => "FLOAT",
        &Type::FLOAT8 => "DOUBLE",
        // NUMERIC is mapped to VARCHAR to preserve precision without loss,
        // since DuckDB DECIMAL needs explicit scale/precision we don't have in the Type.
        &Type::NUMERIC => "VARCHAR",
        &Type::DATE => "DATE",
        &Type::TIME => "TIME",
        &Type::TIMESTAMP => "TIMESTAMP",
        &Type::TIMESTAMPTZ => "TIMESTAMPTZ",
        &Type::UUID => "UUID",
        &Type::JSON | &Type::JSONB => "JSON",
        &Type::OID => "UBIGINT",
        &Type::BYTEA => "BLOB",
        _ => "VARCHAR",
    }
}

/// Returns the DuckDB SQL type string for a given Postgres array type.
fn postgres_array_type_to_duckdb_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::BOOL_ARRAY => "BOOLEAN[]",
        &Type::CHAR_ARRAY
        | &Type::BPCHAR_ARRAY
        | &Type::VARCHAR_ARRAY
        | &Type::NAME_ARRAY
        | &Type::TEXT_ARRAY => "VARCHAR[]",
        &Type::INT2_ARRAY => "SMALLINT[]",
        &Type::INT4_ARRAY => "INTEGER[]",
        &Type::INT8_ARRAY => "BIGINT[]",
        &Type::FLOAT4_ARRAY => "FLOAT[]",
        &Type::FLOAT8_ARRAY => "DOUBLE[]",
        &Type::NUMERIC_ARRAY => "VARCHAR[]",
        &Type::DATE_ARRAY => "DATE[]",
        &Type::TIME_ARRAY => "TIME[]",
        &Type::TIMESTAMP_ARRAY => "TIMESTAMP[]",
        &Type::TIMESTAMPTZ_ARRAY => "TIMESTAMPTZ[]",
        &Type::UUID_ARRAY => "UUID[]",
        &Type::JSON_ARRAY | &Type::JSONB_ARRAY => "JSON[]",
        &Type::OID_ARRAY => "UBIGINT[]",
        &Type::BYTEA_ARRAY => "BLOB[]",
        _ => "VARCHAR[]",
    }
}

/// Returns the DuckDB SQL type string for a Postgres column type.
pub fn postgres_column_type_to_duckdb_sql(typ: &Type) -> &'static str {
    if is_array_type(typ) {
        postgres_array_type_to_duckdb_sql(typ)
    } else {
        postgres_scalar_type_to_duckdb_sql(typ)
    }
}

/// Builds a `CREATE TABLE IF NOT EXISTS` DDL statement for the given table name and schema.
///
/// CDC columns (`cdc_operation` and `cdc_lsn`) are appended at the end and must already
/// be included in `column_schemas` (added by `modify_schema_with_cdc_columns` before calling
/// this function).
pub fn build_create_table_sql(table_name: &str, column_schemas: &[ColumnSchema]) -> String {
    let col_defs: Vec<String> = column_schemas
        .iter()
        .map(|col| {
            let duckdb_type = postgres_column_type_to_duckdb_sql(&col.typ);
            let nullability = if col.nullable { "" } else { " NOT NULL" };
            format!("  \"{}\" {}{}", col.name, duckdb_type, nullability)
        })
        .collect();

    format!(
        "CREATE TABLE IF NOT EXISTS \"{}\" (\n{}\n)",
        table_name,
        col_defs.join(",\n")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_type_mapping() {
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::BOOL), "BOOLEAN");
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::TEXT), "VARCHAR");
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::INT2), "SMALLINT");
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::INT4), "INTEGER");
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::INT8), "BIGINT");
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::FLOAT4), "FLOAT");
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::FLOAT8), "DOUBLE");
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::NUMERIC), "VARCHAR");
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::DATE), "DATE");
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::TIME), "TIME");
        assert_eq!(
            postgres_scalar_type_to_duckdb_sql(&Type::TIMESTAMP),
            "TIMESTAMP"
        );
        assert_eq!(
            postgres_scalar_type_to_duckdb_sql(&Type::TIMESTAMPTZ),
            "TIMESTAMPTZ"
        );
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::UUID), "UUID");
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::JSON), "JSON");
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::JSONB), "JSON");
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::OID), "UBIGINT");
        assert_eq!(postgres_scalar_type_to_duckdb_sql(&Type::BYTEA), "BLOB");
    }

    #[test]
    fn test_array_type_mapping() {
        assert_eq!(
            postgres_array_type_to_duckdb_sql(&Type::BOOL_ARRAY),
            "BOOLEAN[]"
        );
        assert_eq!(
            postgres_array_type_to_duckdb_sql(&Type::TEXT_ARRAY),
            "VARCHAR[]"
        );
        assert_eq!(
            postgres_array_type_to_duckdb_sql(&Type::INT4_ARRAY),
            "INTEGER[]"
        );
        assert_eq!(
            postgres_array_type_to_duckdb_sql(&Type::FLOAT8_ARRAY),
            "DOUBLE[]"
        );
        assert_eq!(
            postgres_array_type_to_duckdb_sql(&Type::UUID_ARRAY),
            "UUID[]"
        );
    }
}
