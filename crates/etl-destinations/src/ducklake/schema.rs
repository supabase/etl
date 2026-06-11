use etl::types::{ColumnSchema, DefaultExpression, Type, is_array_type, parse_default_expression};
use tracing::warn;

use crate::ducklake::sql::{qualified_lake_table_name, quote_identifier};

/// Returns the DuckLake SQL type string for a given Postgres scalar type.
fn postgres_scalar_type_to_ducklake_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::BOOL => "boolean",
        &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => "varchar",
        &Type::INT2 => "smallint",
        &Type::INT4 => "integer",
        &Type::INT8 => "bigint",
        &Type::FLOAT4 => "float",
        &Type::FLOAT8 => "double",
        // numeric is mapped to varchar to preserve precision without loss,
        // since DuckDB decimal needs explicit scale/precision we don't have in the Type.
        &Type::NUMERIC => "varchar",
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
        &Type::CHAR_ARRAY
        | &Type::BPCHAR_ARRAY
        | &Type::VARCHAR_ARRAY
        | &Type::NAME_ARRAY
        | &Type::TEXT_ARRAY => "varchar[]",
        &Type::INT2_ARRAY => "smallint[]",
        &Type::INT4_ARRAY => "integer[]",
        &Type::INT8_ARRAY => "bigint[]",
        &Type::FLOAT4_ARRAY => "float[]",
        &Type::FLOAT8_ARRAY => "double[]",
        &Type::NUMERIC_ARRAY => "varchar[]",
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
fn ducklake_column_definition(column_schema: &ColumnSchema, include_not_null: bool) -> String {
    let column_name = quote_identifier(&column_schema.name);
    let duckdb_type = postgres_column_type_to_ducklake_sql(&column_schema.typ);
    let default_clause = ducklake_default_clause(column_schema).unwrap_or_default();
    let nullability = if include_not_null && !column_schema.nullable { " not null" } else { "" };
    format!("{column_name} {duckdb_type}{default_clause}{nullability}")
}

/// Returns the DuckLake default clause for a column, if supported.
fn ducklake_default_clause(column_schema: &ColumnSchema) -> Option<String> {
    let default_clause = ducklake_default_expression(column_schema)
        .map(|expression| format!(" default {expression}"));
    if default_clause.is_none() && column_schema.default_expression.is_some() {
        warn!(
            column_name = %column_schema.name,
            "skipping unsupported source column default for DuckLake"
        );
    }

    default_clause
}

/// Renders a parsed default expression as DuckLake SQL.
fn render_ducklake_default_expression(expression: &DefaultExpression) -> Option<String> {
    match expression {
        DefaultExpression::StringLiteral(expression)
        | DefaultExpression::NumericLiteral(expression)
        | DefaultExpression::BooleanLiteral(expression)
        | DefaultExpression::DateLiteral(expression)
        | DefaultExpression::TimeLiteral(expression)
        | DefaultExpression::TimestampLiteral(expression)
        | DefaultExpression::JsonLiteral(expression)
        | DefaultExpression::NumericExpression(expression) => Some(expression.clone()),
        DefaultExpression::UuidV4 => Some("uuid()".to_owned()),
        DefaultExpression::CurrentUser => Some("current_user".to_owned()),
        DefaultExpression::CurrentTimestamp | DefaultExpression::TimezoneNow => {
            Some("current_timestamp".to_owned())
        }
        DefaultExpression::CurrentDate => Some("current_date".to_owned()),
        DefaultExpression::CurrentTime => Some("current_time".to_owned()),
        DefaultExpression::LocalTimestamp => Some("localtimestamp".to_owned()),
        DefaultExpression::IntervalArithmetic { base, operator, interval, .. } => {
            let base = render_ducklake_default_expression(base)?;
            Some(format!("{base} {} interval '{}'", operator.as_sql(), interval.literal))
        }
        DefaultExpression::LiteralFunction { function, argument } => {
            Some(format!("{}({argument})", function.as_lower_name()))
        }
    }
}

/// Returns whether a column default can be represented in DuckLake SQL.
pub(super) fn supports_default_ducklake(column_schema: &ColumnSchema) -> bool {
    ducklake_default_expression(column_schema).is_some()
}

/// Returns a rendered DuckLake default expression for a column, if supported.
fn ducklake_default_expression(column_schema: &ColumnSchema) -> Option<String> {
    column_schema.default_expression.as_deref().and_then(|expression| {
        parse_default_expression(expression, &column_schema.typ)
            .and_then(|expression| render_ducklake_default_expression(&expression))
    })
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
        .map(|col| format!("  {}", ducklake_column_definition(col, true)))
        .collect();

    format!("create table if not exists {table_name} ({})", col_defs.join(",\n"))
}

/// Builds a DuckLake `alter table add column` statement.
///
/// Added columns are always nullable at the destination because existing rows
/// cannot be backfilled from the source-side default expression.
pub(super) fn build_add_column_sql_ducklake(
    table_name: &str,
    column_schema: &ColumnSchema,
) -> String {
    let table_name = qualified_lake_table_name(table_name);
    let column_definition = ducklake_column_definition(column_schema, false);

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
    column_schema: &ColumnSchema,
) -> Option<String> {
    let expression = ducklake_default_expression(column_schema)?;
    let table_name = qualified_lake_table_name(table_name);
    let column_name = quote_identifier(&column_schema.name);

    Some(format!("alter table {table_name} alter column {column_name} set default {expression}"))
}

/// Builds a DuckLake `alter table alter column drop default` statement.
pub(super) fn build_drop_default_sql_ducklake(table_name: &str, column_name: &str) -> String {
    let table_name = qualified_lake_table_name(table_name);
    let column_name = quote_identifier(column_name);

    format!("alter table {table_name} alter column {column_name} drop default")
}

/// Builds a DuckLake `alter table alter column set not null` statement.
pub(super) fn build_set_not_null_sql_ducklake(table_name: &str, column_name: &str) -> String {
    let table_name = qualified_lake_table_name(table_name);
    let column_name = quote_identifier(column_name);

    format!("alter table {table_name} alter column {column_name} set not null")
}

/// Builds a DuckLake `alter table alter column drop not null` statement.
pub(super) fn build_drop_not_null_sql_ducklake(table_name: &str, column_name: &str) -> String {
    let table_name = qualified_lake_table_name(table_name);
    let column_name = quote_identifier(column_name);

    format!("alter table {table_name} alter column {column_name} drop not null")
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
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::TIMESTAMP), "timestamp");
        assert_eq!(postgres_scalar_type_to_ducklake_sql(&Type::TIMESTAMPTZ), "timestamptz");
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
        assert_eq!(postgres_array_type_to_ducklake_sql(&Type::UUID_ARRAY), "uuid[]");
    }

    #[test]
    fn build_create_table_sql_qualifies_lake_catalog() {
        let sql = build_create_table_sql_ducklake(
            "odd\"table",
            &[ColumnSchema::new("select".to_owned(), Type::INT4, -1, 1, Some(1), false)],
        );

        assert!(sql.starts_with("create table if not exists \"lake\".\"odd\"\"table\""));
        assert!(sql.contains("  \"select\" integer not null"));
    }

    #[test]
    fn build_add_column_sql_keeps_added_columns_nullable() {
        let sql = build_add_column_sql_ducklake(
            "test_table",
            &ColumnSchema::new("score".to_owned(), Type::INT4, -1, 4, None, false),
        );

        assert_eq!(sql, r#"alter table "lake"."test_table" add column "score" integer"#);
    }

    #[test]
    fn build_add_column_sql_includes_supported_default() {
        let column = ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 4, None, true)
            .with_default_expression(Some("'pending'::text".to_owned()));
        let sql = build_add_column_sql_ducklake("test_table", &column);

        assert_eq!(
            sql,
            r#"alter table "lake"."test_table" add column "status" varchar default 'pending'"#
        );
    }

    #[test]
    fn ducklake_default_clause_renders_portable_expressions() {
        let cases = [
            (Type::JSONB, "'{}'::jsonb", " default '{}'"),
            (Type::UUID, "gen_random_uuid()", " default uuid()"),
            (
                Type::TIMESTAMPTZ,
                "now() + interval '30 days'",
                " default current_timestamp + interval '30 days'",
            ),
            (Type::TEXT, "lower('USER'::text)", " default lower('USER')"),
        ];

        for (typ, expression, expected) in cases {
            let column = ColumnSchema::new("value".to_owned(), typ, -1, 1, None, true)
                .with_default_expression(Some(expression.to_owned()));

            assert_eq!(ducklake_default_clause(&column).as_deref(), Some(expected));
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
        let column = ColumnSchema::new("status\"value".to_owned(), Type::TEXT, -1, 4, None, true)
            .with_default_expression(Some("'pending'::text".to_owned()));

        assert_eq!(
            build_set_default_sql_ducklake("table\"name", &column),
            Some(
                r#"alter table "lake"."table""name" alter column "status""value" set default 'pending'"#
                    .to_owned()
            )
        );
        assert_eq!(
            build_drop_default_sql_ducklake("table\"name", "status\"value"),
            r#"alter table "lake"."table""name" alter column "status""value" drop default"#
        );
        assert_eq!(
            build_set_not_null_sql_ducklake("table\"name", "status\"value"),
            r#"alter table "lake"."table""name" alter column "status""value" set not null"#
        );
        assert_eq!(
            build_drop_not_null_sql_ducklake("table\"name", "status\"value"),
            r#"alter table "lake"."table""name" alter column "status""value" drop not null"#
        );
    }
}
