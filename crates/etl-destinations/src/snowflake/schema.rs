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
        &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => "VARCHAR",
        &Type::INT2 => "SMALLINT",
        &Type::INT4 => "INTEGER",
        &Type::INT8 => "BIGINT",
        &Type::FLOAT4 => "FLOAT",
        &Type::FLOAT8 => "DOUBLE",
        &Type::NUMERIC => "VARCHAR",
        &Type::DATE => "DATE",
        &Type::TIME => "TIME",
        &Type::TIMESTAMP => "TIMESTAMP_NTZ",
        &Type::TIMESTAMPTZ => "TIMESTAMP_TZ",
        &Type::UUID => "VARCHAR",
        &Type::JSON | &Type::JSONB => "VARIANT",
        &Type::OID => "BIGINT",
        &Type::BYTEA => "VARCHAR",
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
    let default_clause = snowflake_default_expression(column_schema)
        .map(|expression| format!(" DEFAULT {expression}"));
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
    default_clause(column_schema)
}

/// Returns whether a column default can be represented in Snowflake SQL.
pub(crate) fn supports_default(column_schema: &ColumnSchema) -> bool {
    snowflake_default_expression(column_schema).is_some()
}

/// Returns a rendered Snowflake default expression for a column, if supported.
fn snowflake_default_expression(column_schema: &ColumnSchema) -> Option<String> {
    column_schema.default_expression.as_deref().and_then(|expression| {
        parse_default_expression(expression, &column_schema.typ).and_then(|expression| {
            render_snowflake_default_expression(&expression, &column_schema.typ)
        })
    })
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
        DefaultExpression::NumericExpression(expression) => {
            is_snowflake_numeric_default_type(typ).then(|| expression.clone())
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
        DefaultExpression::TimestampLiteral(expression) => {
            is_snowflake_timestamp_default_type(typ).then(|| expression.clone())
        }
        DefaultExpression::JsonLiteral(expression) => {
            is_json_type(typ).then(|| format!("PARSE_JSON({expression})"))
        }
        DefaultExpression::UuidV4 => {
            is_snowflake_uuid_default_type(typ).then(|| "UUID_STRING()".to_owned())
        }
        DefaultExpression::CurrentUser => {
            is_snowflake_text_default_type(typ).then(|| "CURRENT_USER()".to_owned())
        }
        DefaultExpression::CurrentTimestamp
        | DefaultExpression::LocalTimestamp
        | DefaultExpression::TimezoneNow => render_snowflake_current_timestamp_default(typ),
        DefaultExpression::CurrentDate => {
            matches!(typ, &Type::DATE).then(|| "CURRENT_DATE()".to_owned())
        }
        DefaultExpression::CurrentTime => {
            matches!(typ, &Type::TIME).then(|| "CURRENT_TIME()".to_owned())
        }
        DefaultExpression::IntervalArithmetic { base, operator, interval, .. } => {
            let base = render_snowflake_default_expression(base, typ)?;
            Some(format!("{base} {} INTERVAL '{}'", operator.as_sql(), interval.literal))
        }
        DefaultExpression::LiteralFunction { function, argument } => {
            is_snowflake_text_default_type(typ)
                .then(|| format!("{}({argument})", function.as_upper_name()))
        }
    }
}

/// Renders timestamp-like Postgres defaults for the destination column type.
fn render_snowflake_current_timestamp_default(typ: &Type) -> Option<String> {
    match typ {
        &Type::DATE => Some("CURRENT_DATE()".to_owned()),
        &Type::TIME => Some("CURRENT_TIME()".to_owned()),
        &Type::TIMESTAMP | &Type::TIMESTAMPTZ => Some("CURRENT_TIMESTAMP()".to_owned()),
        _ => None,
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
        || matches!(typ, &Type::NUMERIC | &Type::MONEY | &Type::UUID)
}

/// Returns whether a Postgres type is a text-like Snowflake VARCHAR column.
fn is_snowflake_text_default_type(typ: &Type) -> bool {
    matches!(typ, &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT)
}

/// Returns whether a Postgres type can safely receive UUID-producing defaults.
fn is_snowflake_uuid_default_type(typ: &Type) -> bool {
    matches!(
        typ,
        &Type::UUID | &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT
    )
}

/// Returns whether a Postgres type is a Snowflake timestamp column.
fn is_snowflake_timestamp_default_type(typ: &Type) -> bool {
    matches!(typ, &Type::TIMESTAMP | &Type::TIMESTAMPTZ)
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
            (&Type::TIMESTAMP, "TIMESTAMP_NTZ"),
            (&Type::TIMESTAMPTZ, "TIMESTAMP_TZ"),
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
            (Type::UUID, "gen_random_uuid()", " DEFAULT UUID_STRING()"),
            (Type::TEXT, "CURRENT_USER", " DEFAULT CURRENT_USER()"),
            (Type::NUMERIC, "42", " DEFAULT '42'"),
            (Type::DATE, "now()", " DEFAULT CURRENT_DATE()"),
            (Type::TIME, "now()", " DEFAULT CURRENT_TIME()"),
            (
                Type::TIMESTAMPTZ,
                "now() + interval '30 days'",
                " DEFAULT CURRENT_TIMESTAMP() + INTERVAL '30 days'",
            ),
            (Type::TEXT, "upper('user'::text)", " DEFAULT UPPER('user')"),
        ];

        for (typ, expression, expected) in cases {
            let column = ColumnSchema::new("value".to_owned(), typ, -1, 1, true)
                .with_default_expression(expression.to_owned());

            assert_eq!(default_clause(&column).as_deref(), Some(expected));
        }

        let unsupported_cases = [
            (Type::INT4, "'abc'::text"),
            (Type::NUMERIC, "10 + 5"),
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
        let supported = ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 1, true)
            .with_default_expression("'pending'::text".to_owned());
        let supported_function = ColumnSchema::new("value".to_owned(), Type::UUID, -1, 1, true)
            .with_default_expression("gen_random_uuid()".to_owned());
        let supported_json = ColumnSchema::new("value".to_owned(), Type::JSONB, -1, 1, true)
            .with_default_expression("'{}'::jsonb".to_owned());
        let unsupported_numeric_expression =
            ColumnSchema::new("value".to_owned(), Type::NUMERIC, -1, 1, true)
                .with_default_expression("10 + 5".to_owned());

        assert_eq!(add_column_default_clause(&supported).as_deref(), Some(" DEFAULT 'pending'"));
        assert_eq!(
            add_column_default_clause(&supported_function).as_deref(),
            Some(" DEFAULT UUID_STRING()")
        );
        assert_eq!(
            add_column_default_clause(&supported_json).as_deref(),
            Some(" DEFAULT PARSE_JSON('{}')")
        );
        assert_eq!(add_column_default_clause(&unsupported_numeric_expression), None);
    }
}
