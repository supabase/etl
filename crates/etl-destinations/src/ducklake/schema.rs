use etl::types::{ColumnSchema, DefaultExpression, Type, is_array_type, parse_default_expression};
use tracing::warn;

use crate::ducklake::sql::{qualified_lake_table_name, quote_identifier};

/// Returns the DuckLake SQL type string for a given Postgres scalar type.
fn postgres_scalar_type_to_ducklake_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::BOOL => "boolean",
        &Type::INT2 => "smallint",
        &Type::INT4 => "integer",
        &Type::INT8 => "bigint",
        &Type::FLOAT4 => "float",
        &Type::FLOAT8 => "double",
        &Type::DATE => "date",
        &Type::TIME => "time",
        &Type::TIMESTAMP => "timestamp",
        &Type::TIMESTAMPTZ => "timestamptz",
        &Type::UUID => "uuid",
        &Type::JSON | &Type::JSONB => "json",
        &Type::OID => "ubigint",
        &Type::BYTEA => "blob",
        _ => "varchar",
    }
}

/// Returns the DuckDB SQL type string for a given Postgres array type.
fn postgres_array_type_to_ducklake_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::BOOL_ARRAY => "boolean[]",
        &Type::INT2_ARRAY => "smallint[]",
        &Type::INT4_ARRAY => "integer[]",
        &Type::INT8_ARRAY => "bigint[]",
        &Type::FLOAT4_ARRAY => "float[]",
        &Type::FLOAT8_ARRAY => "double[]",
        &Type::DATE_ARRAY => "date[]",
        &Type::TIME_ARRAY => "time[]",
        &Type::TIMESTAMP_ARRAY => "timestamp[]",
        &Type::TIMESTAMPTZ_ARRAY => "timestamptz[]",
        &Type::UUID_ARRAY => "uuid[]",
        &Type::JSON_ARRAY | &Type::JSONB_ARRAY => "json[]",
        &Type::OID_ARRAY => "ubigint[]",
        &Type::BYTEA_ARRAY => "blob[]",
        _ => "varchar[]",
    }
}

/// Returns the DuckLake SQL type string for a Postgres column type.
fn postgres_column_type_to_ducklake_sql(typ: &Type) -> &'static str {
    if is_array_type(typ) {
        postgres_array_type_to_ducklake_sql(typ)
    } else {
        postgres_scalar_type_to_ducklake_sql(typ)
    }
}

/// Builds one DuckLake column definition.
///
/// For example, a non-null source `name text` column becomes
/// `"name" varchar not null`.
fn ducklake_column_definition(
    column_schema: &ColumnSchema,
    include_default: bool,
    include_not_null: bool,
) -> String {
    let column_name = quote_identifier(&column_schema.name);
    let duckdb_type = postgres_column_type_to_ducklake_sql(&column_schema.typ);
    let default_clause = if include_default {
        ducklake_default_clause(column_schema).unwrap_or_default()
    } else {
        String::new()
    };
    let nullability = if include_not_null && !column_schema.nullable { " not null" } else { "" };
    format!("{column_name} {duckdb_type}{default_clause}{nullability}")
}

/// Returns the DuckLake default clause for a column, if supported.
fn ducklake_default_clause(column_schema: &ColumnSchema) -> Option<String> {
    let default_clause = column_schema
        .default_expression
        .as_deref()
        .and_then(|default_expression| {
            ducklake_default_expression(default_expression, &column_schema.typ)
        })
        .map(|rendered_default_expression| format!(" default {rendered_default_expression}"));
    if default_clause.is_none() && column_schema.default_expression.is_some() {
        warn!(
            column_name = %column_schema.name,
            "skipping unsupported source column default for DuckLake"
        );
    }

    default_clause
}

/// Renders a parsed default expression as DuckLake SQL.
fn render_ducklake_default_expression(
    expression: &DefaultExpression,
    typ: &Type,
) -> Option<String> {
    match expression {
        DefaultExpression::StringLiteral(expression) => {
            is_ducklake_string_default_type(typ).then(|| expression.clone())
        }
        DefaultExpression::NumericLiteral(expression) => {
            if is_ducklake_numeric_default_type(typ) {
                Some(expression.clone())
            } else if is_ducklake_numeric_string_default_type(typ) {
                Some(quote_numeric_literal_as_string(expression))
            } else {
                None
            }
        }
        DefaultExpression::DateLiteral(expression) => {
            matches!(typ, &Type::DATE).then(|| expression.clone())
        }
        DefaultExpression::TimeLiteral(expression) => {
            matches!(typ, &Type::TIME).then(|| expression.clone())
        }
        DefaultExpression::TimeTzLiteral(expression) => {
            matches!(typ, &Type::TIMETZ).then(|| expression.clone())
        }
        DefaultExpression::TimestampLiteral(expression) => {
            matches!(typ, &Type::TIMESTAMP).then(|| expression.clone())
        }
        DefaultExpression::TimestampTzLiteral(expression) => {
            matches!(typ, &Type::TIMESTAMPTZ).then(|| expression.clone())
        }
        DefaultExpression::IntervalLiteral(expression) => {
            matches!(typ, &Type::INTERVAL).then(|| expression.clone())
        }
        DefaultExpression::JsonLiteral(expression) => is_json_type(typ).then(|| expression.clone()),
        DefaultExpression::BooleanLiteral(_) => None,
    }
}

/// Returns whether a Postgres type is a DuckLake numeric column.
fn is_ducklake_numeric_default_type(typ: &Type) -> bool {
    matches!(
        typ,
        &Type::INT2 | &Type::INT4 | &Type::INT8 | &Type::FLOAT4 | &Type::FLOAT8 | &Type::OID
    )
}

/// Returns whether a Postgres numeric-like type is stored as DuckLake varchar.
fn is_ducklake_numeric_string_default_type(typ: &Type) -> bool {
    matches!(typ, &Type::NUMERIC)
}

/// Returns whether a Postgres type can safely receive source string literals.
fn is_ducklake_string_default_type(typ: &Type) -> bool {
    is_ducklake_text_default_type(typ)
        || matches!(typ, &Type::NUMERIC | &Type::TIMETZ | &Type::INTERVAL | &Type::UUID)
}

/// Returns whether a Postgres type is a text-like DuckLake varchar column.
fn is_ducklake_text_default_type(typ: &Type) -> bool {
    matches!(typ, &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT)
}

/// Returns whether a Postgres type is a DuckLake JSON column.
fn is_json_type(typ: &Type) -> bool {
    matches!(typ, &Type::JSON | &Type::JSONB)
}

/// Quotes a parser-validated numeric literal as a SQL string literal.
fn quote_numeric_literal_as_string(expression: &str) -> String {
    format!("'{expression}'")
}

/// Returns whether a column default can be represented in DuckLake SQL.
pub(super) fn supports_column_default_ducklake(default_expression: &str, typ: &Type) -> bool {
    ducklake_default_expression(default_expression, typ).is_some()
}

/// Returns a rendered DuckLake default expression for a column, if supported.
fn ducklake_default_expression(default_expression: &str, typ: &Type) -> Option<String> {
    parse_default_expression(default_expression, typ)
        .and_then(|expression| render_ducklake_default_expression(&expression, typ))
}

/// Builds a `create table if not exists` DDL statement for the given table name
/// and schema.
///
/// The supplied columns are the destination-visible replicated columns in
/// write order.
pub(super) fn build_create_table_sql_ducklake(
    table_name: &str,
    column_schemas: &[ColumnSchema],
) -> String {
    let table_name = qualified_lake_table_name(table_name);
    let col_defs: Vec<String> = column_schemas
        .iter()
        .map(|col| format!("  {}", ducklake_column_definition(col, true, true)))
        .collect();

    format!("create table if not exists {table_name} ({})", col_defs.join(",\n"))
}

/// Builds a DuckLake `alter table add column` statement.
///
/// DuckLake stores add-time defaults as metadata and does not rewrite data
/// files during schema evolution.
pub(super) fn build_add_column_sql_ducklake(
    table_name: &str,
    column_schema: &ColumnSchema,
) -> String {
    let table_name = qualified_lake_table_name(table_name);
    let column_definition = ducklake_column_definition(column_schema, true, false);

    format!("alter table {table_name} add column {column_definition}")
}

/// Builds a DuckLake `alter table drop column` statement.
pub(super) fn build_drop_column_sql_ducklake(table_name: &str, column_name: &str) -> String {
    let table_name = qualified_lake_table_name(table_name);
    let column_name = quote_identifier(column_name);

    format!("alter table {table_name} drop column {column_name}")
}

/// Builds a DuckLake `alter table rename column` statement.
pub(super) fn build_rename_column_sql_ducklake(
    table_name: &str,
    old_name: &str,
    new_name: &str,
) -> String {
    let table_name = qualified_lake_table_name(table_name);
    let old_name = quote_identifier(old_name);
    let new_name = quote_identifier(new_name);

    format!("alter table {table_name} rename column {old_name} to {new_name}")
}

/// Builds a DuckLake `alter table alter column set default` statement.
pub(super) fn build_set_default_sql_ducklake(
    table_name: &str,
    column_name: &str,
    typ: &Type,
    default_expression: &str,
) -> Option<String> {
    let rendered_default_expression = ducklake_default_expression(default_expression, typ)?;
    let table_name = qualified_lake_table_name(table_name);
    let column_name = quote_identifier(column_name);

    Some(format!(
        "alter table {table_name} alter column {column_name} set default \
         {rendered_default_expression}"
    ))
}

/// Builds a DuckLake `alter table alter column drop default` statement.
pub(super) fn build_drop_default_sql_ducklake(table_name: &str, column_name: &str) -> String {
    let table_name = qualified_lake_table_name(table_name);
    let column_name = quote_identifier(column_name);

    format!("alter table {table_name} alter column {column_name} drop default")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_type_mapping() {
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::BOOL), "boolean");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::TEXT), "varchar");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::INT2), "smallint");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::INT4), "integer");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::INT8), "bigint");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::FLOAT4), "float");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::FLOAT8), "double");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::NUMERIC), "varchar");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::DATE), "date");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::TIME), "time");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::TIMETZ), "varchar");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::TIMESTAMP), "timestamp");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::TIMESTAMPTZ), "timestamptz");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::INTERVAL), "varchar");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::UUID), "uuid");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::JSON), "json");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::JSONB), "json");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::OID), "ubigint");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::BYTEA), "blob");
    }

    #[test]
    fn array_type_mapping() {
        assert_eq!(postgres_array_type_to_ducklake_sql(&Type::BOOL_ARRAY), "boolean[]");
        assert_eq!(postgres_array_type_to_ducklake_sql(&Type::TEXT_ARRAY), "varchar[]");
        assert_eq!(postgres_array_type_to_ducklake_sql(&Type::INT4_ARRAY), "integer[]");
        assert_eq!(postgres_array_type_to_ducklake_sql(&Type::FLOAT8_ARRAY), "double[]");
        assert_eq!(postgres_array_type_to_ducklake_sql(&Type::TIMETZ_ARRAY), "varchar[]");
        assert_eq!(postgres_array_type_to_ducklake_sql(&Type::INTERVAL_ARRAY), "varchar[]");
        assert_eq!(postgres_array_type_to_ducklake_sql(&Type::UUID_ARRAY), "uuid[]");
    }

    #[test]
    fn build_create_table_sql_qualifies_lake_catalog() {
        let sql = build_create_table_sql_ducklake(
            "odd\"table",
            &[ColumnSchema::new("select".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1)],
        );

        assert!(sql.starts_with("create table if not exists \"lake\".\"odd\"\"table\""));
        assert!(sql.contains("  \"select\" integer not null"));
    }

    #[test]
    fn build_add_column_sql_keeps_added_columns_nullable() {
        let sql = build_add_column_sql_ducklake(
            "test_table",
            &ColumnSchema::new("score".to_owned(), Type::INT4, -1, 4, false),
        );

        assert_eq!(sql, r#"alter table "lake"."test_table" add column "score" integer"#);
    }

    #[test]
    fn build_add_column_sql_includes_supported_default() {
        let column = ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 4, true)
            .with_default_expression("'pending'::text".to_owned());
        let sql = build_add_column_sql_ducklake("test_table", &column);

        assert_eq!(
            sql,
            r#"alter table "lake"."test_table" add column "status" varchar default 'pending'"#
        );
    }

    #[test]
    fn ducklake_default_clause_renders_portable_expressions() {
        let cases = [
            (Type::TEXT, "true", " default 'true'"),
            (Type::JSONB, "'{}'::jsonb", " default '{}'"),
            (Type::NUMERIC, "42", " default '42'"),
            (Type::TIME, "'12:30:00'::time", " default '12:30:00'"),
            (Type::TIMETZ, "'12:30:00+02'::timetz", " default '12:30:00+02'"),
            (Type::INTERVAL, "'30 days'::interval", " default '30 days'"),
            (
                Type::TIMESTAMPTZ,
                "'2026-01-01 12:30:00'::timestamptz",
                " default '2026-01-01 12:30:00'",
            ),
            (
                Type::UUID,
                "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid",
                " default 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
            ),
        ];

        for (typ, expression, expected) in cases {
            let column = ColumnSchema::new("value".to_owned(), typ, -1, 1, true)
                .with_default_expression(expression.to_owned());

            assert_eq!(ducklake_default_clause(&column).as_deref(), Some(expected));
        }

        let unsupported_cases = [
            (Type::BOOL, "true"),
            (Type::BOOL, "'true'::text"),
            (Type::INT4, "'abc'::text"),
            (Type::NUMERIC, "10 + 5"),
            (Type::UUID, "gen_random_uuid()"),
            (Type::DATE, "now()"),
            (Type::TIME, "now()"),
            (Type::TIMESTAMPTZ, "now() + interval '30 days'"),
            (Type::TEXT, "lower('USER'::text)"),
            (Type::TEXT, "current_date"),
            (Type::DATE, "current_time"),
        ];
        for (typ, expression) in unsupported_cases {
            let column = ColumnSchema::new("value".to_owned(), typ, -1, 1, true)
                .with_default_expression(expression.to_owned());

            assert_eq!(ducklake_default_clause(&column), None);
        }
    }

    #[test]
    fn build_drop_column_sql_quotes_identifiers() {
        let sql = build_drop_column_sql_ducklake("table\"name", "old\"column");

        assert_eq!(sql, r#"alter table "lake"."table""name" drop column "old""column""#);
    }

    #[test]
    fn build_rename_column_sql_quotes_identifiers() {
        let sql = build_rename_column_sql_ducklake("table\"name", "old\"column", "new\"column");

        assert_eq!(
            sql,
            r#"alter table "lake"."table""name" rename column "old""column" to "new""column""#
        );
    }

    #[test]
    fn build_column_update_sql_quotes_identifiers() {
        let column = ColumnSchema::new("status\"value".to_owned(), Type::TEXT, -1, 4, true)
            .with_default_expression("'pending'::text".to_owned());

        assert_eq!(
            build_set_default_sql_ducklake(
                "table\"name",
                &column.name,
                &column.typ,
                column.default_expression.as_deref().expect("test default")
            ),
            Some(
                r#"alter table "lake"."table""name" alter column "status""value" set default 'pending'"#
                    .to_owned()
            )
        );
        assert_eq!(
            build_drop_default_sql_ducklake("table\"name", "status\"value"),
            r#"alter table "lake"."table""name" alter column "status""value" drop default"#
        );
    }
}
