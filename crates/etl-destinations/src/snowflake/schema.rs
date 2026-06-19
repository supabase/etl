use etl::types::{ColumnSchema, DefaultExpression, Type, is_array_type, parse_default_expression};
use tracing::warn;

use crate::snowflake::{Error, Result, sql::quote_identifier};

pub(crate) const CDC_OPERATION_COLUMN: &str = "_cdc_operation";
pub(crate) const CDC_SEQUENCE_COLUMN: &str = "_cdc_sequence_number";

/// Returns the Snowflake DDL type string for a given Postgres type.
///
/// Array types map to ARRAY (Snowflake's native array type, a subtype of
/// VARIANT). Scalar types follow the closest Snowflake equivalent.
pub(crate) fn type_name(typ: &Type) -> &'static str {
    if is_array_type(typ) {
        return "ARRAY";
    }

    match typ {
        &Type::BOOL => "BOOLEAN",
        &Type::INT2 => "SMALLINT",
        &Type::INT4 => "INTEGER",
        &Type::INT8 => "BIGINT",
        &Type::FLOAT4 => "FLOAT",
        &Type::FLOAT8 => "DOUBLE",
        &Type::DATE => "DATE",
        &Type::TIME => "TIME",
        &Type::TIMESTAMP => "TIMESTAMP_NTZ",
        &Type::TIMESTAMPTZ => "TIMESTAMP_TZ",
        &Type::JSON | &Type::JSONB => "VARIANT",
        &Type::OID => "BIGINT",
        _ => "VARCHAR",
    }
}

/// Checks that no source column collides with the reserved CDC column names.
pub(crate) fn validate_no_cdc_collisions(columns: &[ColumnSchema]) -> Result<()> {
    for col in columns {
        if col.name == CDC_OPERATION_COLUMN || col.name == CDC_SEQUENCE_COLUMN {
            return Err(Error::Config(format!(
                "source column '{}' collides with reserved CDC column name",
                col.name,
            )));
        }
    }
    Ok(())
}

/// Builds the column definitions string.
///
/// Each source column is rendered as `"name" TYPE` + two CDC columns are
/// appended at the end.
pub(crate) fn build_column_defs(columns: &[ColumnSchema]) -> String {
    let mut parts: Vec<String> = columns
        .iter()
        .map(|col| {
            let default_clause = default_clause(col).unwrap_or_default();
            format!("{} {}{}", quote_identifier(&col.name), type_name(&col.typ), default_clause)
        })
        .collect();

    parts.push(format!("{} VARCHAR NOT NULL", quote_identifier(CDC_OPERATION_COLUMN)));
    parts.push(format!("{} VARCHAR NOT NULL", quote_identifier(CDC_SEQUENCE_COLUMN)));

    parts.join(", ")
}

/// Returns the Snowflake default clause for a column, if supported.
pub(crate) fn default_clause(column_schema: &ColumnSchema) -> Option<String> {
    let default_clause = column_schema
        .default_expression
        .as_deref()
        .and_then(|default_expression| {
            snowflake_default_expression(default_expression, &column_schema.typ)
        })
        .map(|rendered_default_expression| format!(" DEFAULT {rendered_default_expression}"));
    if default_clause.is_none() && column_schema.default_expression.is_some() {
        warn!(
            column_name = %column_schema.name,
            "skipping unsupported source column default for Snowflake"
        );
    }

    default_clause
}

/// Returns the Snowflake default clause to include in `ADD COLUMN`, if
/// supported.
pub(crate) fn add_column_default_clause(column_schema: &ColumnSchema) -> Option<String> {
    let default_clause = column_schema
        .default_expression
        .as_deref()
        .and_then(|default_expression| {
            snowflake_add_column_default_expression(default_expression, &column_schema.typ)
        })
        .map(|rendered_default_expression| format!(" DEFAULT {rendered_default_expression}"));
    if default_clause.is_none() && column_schema.default_expression.is_some() {
        warn!(
            column_name = %column_schema.name,
            "skipping source column default for Snowflake ADD COLUMN because Snowflake only \
             supports literal add-column defaults"
        );
    }

    default_clause
}

/// Returns whether a column default can be represented in Snowflake SQL.
pub(crate) fn supports_column_default(default_expression: &str, typ: &Type) -> bool {
    snowflake_default_expression(default_expression, typ).is_some()
}

/// Returns a rendered Snowflake default expression for a column, if supported.
fn snowflake_default_expression(default_expression: &str, typ: &Type) -> Option<String> {
    parse_default_expression(default_expression, typ)
        .and_then(|expression| render_snowflake_default_expression(&expression, typ))
}

/// Returns a Snowflake `ADD COLUMN` default expression, if supported.
fn snowflake_add_column_default_expression(default_expression: &str, typ: &Type) -> Option<String> {
    parse_default_expression(default_expression, typ)
        .and_then(|expression| render_snowflake_add_column_default_expression(&expression, typ))
}

/// Renders a parsed default expression as Snowflake SQL.
fn render_snowflake_default_expression(
    expression: &DefaultExpression,
    typ: &Type,
) -> Option<String> {
    match expression {
        DefaultExpression::StringLiteral(expression) => {
            is_snowflake_string_default_type(typ).then(|| expression.clone())
        }
        DefaultExpression::NumericLiteral(expression) => {
            if is_snowflake_numeric_default_type(typ) {
                Some(expression.clone())
            } else if is_snowflake_numeric_string_default_type(typ) {
                Some(quote_numeric_literal_as_string(expression))
            } else {
                None
            }
        }
        DefaultExpression::BooleanLiteral(expression) => {
            matches!(typ, &Type::BOOL).then(|| expression.clone())
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
        DefaultExpression::JsonLiteral(expression) => {
            is_json_type(typ).then(|| format!("PARSE_JSON({expression})"))
        }
    }
}

/// Renders a parsed default expression for Snowflake `ADD COLUMN`.
fn render_snowflake_add_column_default_expression(
    expression: &DefaultExpression,
    typ: &Type,
) -> Option<String> {
    match expression {
        DefaultExpression::StringLiteral(expression) => {
            is_snowflake_string_default_type(typ).then(|| expression.clone())
        }
        DefaultExpression::NumericLiteral(expression) => {
            if is_snowflake_numeric_default_type(typ) {
                Some(expression.clone())
            } else if is_snowflake_numeric_string_default_type(typ) {
                Some(quote_numeric_literal_as_string(expression))
            } else {
                None
            }
        }
        DefaultExpression::BooleanLiteral(expression) => {
            matches!(typ, &Type::BOOL).then(|| expression.clone())
        }
        DefaultExpression::DateLiteral(_)
        | DefaultExpression::TimeLiteral(_)
        | DefaultExpression::TimeTzLiteral(_)
        | DefaultExpression::TimestampLiteral(_)
        | DefaultExpression::TimestampTzLiteral(_)
        | DefaultExpression::IntervalLiteral(_)
        | DefaultExpression::JsonLiteral(_) => None,
    }
}

/// Returns whether a Postgres type is a Snowflake numeric column.
fn is_snowflake_numeric_default_type(typ: &Type) -> bool {
    matches!(
        typ,
        &Type::INT2 | &Type::INT4 | &Type::INT8 | &Type::FLOAT4 | &Type::FLOAT8 | &Type::OID
    )
}

/// Returns whether a Postgres numeric-like type is stored as Snowflake VARCHAR.
fn is_snowflake_numeric_string_default_type(typ: &Type) -> bool {
    matches!(typ, &Type::NUMERIC | &Type::MONEY)
}

/// Returns whether a Postgres type can safely receive source string literals.
fn is_snowflake_string_default_type(typ: &Type) -> bool {
    is_snowflake_text_default_type(typ)
        || matches!(
            typ,
            &Type::NUMERIC | &Type::MONEY | &Type::TIMETZ | &Type::INTERVAL | &Type::UUID
        )
}

/// Returns whether a Postgres type is a text-like Snowflake VARCHAR column.
fn is_snowflake_text_default_type(typ: &Type) -> bool {
    matches!(typ, &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT)
}

/// Returns whether a Postgres type is a Snowflake VARIANT JSON column.
fn is_json_type(typ: &Type) -> bool {
    matches!(typ, &Type::JSON | &Type::JSONB)
}

/// Quotes a parser-validated numeric literal as a SQL string literal.
fn quote_numeric_literal_as_string(expression: &str) -> String {
    format!("'{expression}'")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> ColumnSchema {
        ColumnSchema::new(name.to_owned(), Type::INT4, -1, 1, true)
    }

    #[test]
    fn type_mapping() {
        let cases: &[(&Type, &str)] = &[
            // Scalars
            (&Type::BOOL, "BOOLEAN"),
            (&Type::CHAR, "VARCHAR"),
            (&Type::BPCHAR, "VARCHAR"),
            (&Type::VARCHAR, "VARCHAR"),
            (&Type::NAME, "VARCHAR"),
            (&Type::TEXT, "VARCHAR"),
            (&Type::INT2, "SMALLINT"),
            (&Type::INT4, "INTEGER"),
            (&Type::INT8, "BIGINT"),
            (&Type::FLOAT4, "FLOAT"),
            (&Type::FLOAT8, "DOUBLE"),
            (&Type::NUMERIC, "VARCHAR"),
            (&Type::DATE, "DATE"),
            (&Type::TIME, "TIME"),
            (&Type::TIMETZ, "VARCHAR"),
            (&Type::TIMESTAMP, "TIMESTAMP_NTZ"),
            (&Type::TIMESTAMPTZ, "TIMESTAMP_TZ"),
            (&Type::INTERVAL, "VARCHAR"),
            (&Type::UUID, "VARCHAR"),
            (&Type::JSON, "VARIANT"),
            (&Type::JSONB, "VARIANT"),
            (&Type::OID, "BIGINT"),
            (&Type::BYTEA, "VARCHAR"),
            // Arrays all map to VARIANT
            (&Type::BOOL_ARRAY, "ARRAY"),
            (&Type::INT4_ARRAY, "ARRAY"),
            (&Type::TEXT_ARRAY, "ARRAY"),
            (&Type::JSONB_ARRAY, "ARRAY"),
            (&Type::BYTEA_ARRAY, "ARRAY"),
            // Unknown falls back to VARCHAR
            (&Type::BIT, "VARCHAR"),
        ];
        for (typ, expected) in cases {
            assert_eq!(type_name(typ), *expected, "type: {typ:?}");
        }
    }

    #[test]
    fn cdc_collision_validation() {
        let cases: &[(&[&str], bool)] = &[
            (&["id", "_cdc_operation"], true),
            (&["id", "_cdc_sequence_number"], true),
            (&["_cdc_operation", "_cdc_sequence_number"], true),
            (&["id", "name", "_custom_cdc"], false),
        ];
        for (col_names, should_err) in cases {
            let columns: Vec<_> = col_names.iter().map(|n| col(n)).collect();
            assert_eq!(
                validate_no_cdc_collisions(&columns).is_err(),
                *should_err,
                "columns: {col_names:?}"
            );
        }
    }

    #[test]
    fn build_column_defs_output() {
        let columns = vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, true),
            ColumnSchema::new("created_at".to_owned(), Type::TIMESTAMPTZ, -1, 2, true),
        ];
        let defs = build_column_defs(&columns);
        assert_eq!(
            defs,
            r#""id" INTEGER, "created_at" TIMESTAMP_TZ, "_cdc_operation" VARCHAR NOT NULL, "_cdc_sequence_number" VARCHAR NOT NULL"#
        );
    }

    #[test]
    fn build_column_defs_includes_supported_default() {
        let columns = vec![
            ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 1, true)
                .with_default_expression("'pending'::text".to_owned()),
            ColumnSchema::new("payload".to_owned(), Type::JSONB, -1, 2, true)
                .with_default_expression("'{}'::jsonb".to_owned()),
        ];

        let defs = build_column_defs(&columns);

        assert_eq!(
            defs,
            r#""status" VARCHAR DEFAULT 'pending', "payload" VARIANT DEFAULT PARSE_JSON('{}'), "_cdc_operation" VARCHAR NOT NULL, "_cdc_sequence_number" VARCHAR NOT NULL"#
        );
    }

    #[test]
    fn default_clause_renders_portable_expressions() {
        let cases = [
            (Type::TEXT, "true", " DEFAULT 'true'"),
            (Type::BOOL, "'true'::text", " DEFAULT true"),
            (Type::NUMERIC, "42", " DEFAULT '42'"),
            (Type::TIME, "'12:30:00'::time", " DEFAULT '12:30:00'"),
            (Type::TIMETZ, "'12:30:00+02'::timetz", " DEFAULT '12:30:00+02'"),
            (Type::INTERVAL, "'30 days'::interval", " DEFAULT '30 days'"),
            (
                Type::TIMESTAMPTZ,
                "'2026-01-01 12:30:00'::timestamptz",
                " DEFAULT '2026-01-01 12:30:00'",
            ),
            (
                Type::UUID,
                "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid",
                " DEFAULT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
            ),
        ];

        for (typ, expression, expected) in cases {
            let column = ColumnSchema::new("value".to_owned(), typ, -1, 1, true)
                .with_default_expression(expression.to_owned());

            assert_eq!(default_clause(&column).as_deref(), Some(expected));
        }

        let unsupported_cases = [
            (Type::INT4, "'abc'::text"),
            (Type::NUMERIC, "10 + 5"),
            (Type::UUID, "gen_random_uuid()"),
            (Type::TEXT, "CURRENT_USER"),
            (Type::DATE, "now()"),
            (Type::TIME, "now()"),
            (Type::TIMESTAMPTZ, "now() + interval '30 days'"),
            (Type::TEXT, "upper('user'::text)"),
            (Type::TEXT, "current_date"),
            (Type::DATE, "current_time"),
        ];
        for (typ, expression) in unsupported_cases {
            let column = ColumnSchema::new("value".to_owned(), typ, -1, 1, true)
                .with_default_expression(expression.to_owned());

            assert_eq!(default_clause(&column), None);
        }
    }

    #[test]
    fn add_column_default_clause_renders_supported_expressions() {
        let supported_text = ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 1, true)
            .with_default_expression("'pending'::text".to_owned());
        let supported_integer = ColumnSchema::new("value".to_owned(), Type::INT4, -1, 1, true)
            .with_default_expression("15".to_owned());
        let supported_boolean = ColumnSchema::new("value".to_owned(), Type::BOOL, -1, 1, true)
            .with_default_expression("true".to_owned());
        let supported_numeric_as_string =
            ColumnSchema::new("value".to_owned(), Type::NUMERIC, -1, 1, true)
                .with_default_expression("42".to_owned());
        let unsupported_function = ColumnSchema::new("value".to_owned(), Type::UUID, -1, 1, true)
            .with_default_expression("gen_random_uuid()".to_owned());
        let unsupported_json = ColumnSchema::new("value".to_owned(), Type::JSONB, -1, 1, true)
            .with_default_expression("'{}'::jsonb".to_owned());
        let unsupported_timetz = ColumnSchema::new("value".to_owned(), Type::TIMETZ, -1, 1, true)
            .with_default_expression("'12:30:00+02'::timetz".to_owned());
        let unsupported_current_timestamp =
            ColumnSchema::new("value".to_owned(), Type::TIMESTAMPTZ, -1, 1, true)
                .with_default_expression("now()".to_owned());
        let unsupported_numeric_expression =
            ColumnSchema::new("value".to_owned(), Type::INT4, -1, 1, true)
                .with_default_expression("10 + 5".to_owned());

        assert_eq!(
            add_column_default_clause(&supported_text).as_deref(),
            Some(" DEFAULT 'pending'")
        );
        assert_eq!(add_column_default_clause(&supported_integer).as_deref(), Some(" DEFAULT 15"));
        assert_eq!(add_column_default_clause(&supported_boolean).as_deref(), Some(" DEFAULT true"));
        assert_eq!(
            add_column_default_clause(&supported_numeric_as_string).as_deref(),
            Some(" DEFAULT '42'")
        );
        assert_eq!(add_column_default_clause(&unsupported_function), None);
        assert_eq!(add_column_default_clause(&unsupported_json), None);
        assert_eq!(add_column_default_clause(&unsupported_timetz), None);
        assert_eq!(add_column_default_clause(&unsupported_current_timestamp), None);
        assert_eq!(add_column_default_clause(&unsupported_numeric_expression), None);
    }
}
