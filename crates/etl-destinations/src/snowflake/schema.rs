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

/// Returns whether a column default can be represented in Snowflake SQL.
pub(crate) fn supports_default(column_schema: &ColumnSchema) -> bool {
    snowflake_default_expression(column_schema).is_some()
}

/// Returns a rendered Snowflake default expression for a column, if supported.
fn snowflake_default_expression(column_schema: &ColumnSchema) -> Option<String> {
    column_schema.default_expression.as_deref().and_then(|expression| {
        parse_default_expression(expression, &column_schema.typ)
            .and_then(|expression| render_snowflake_default_expression(&expression))
    })
}

/// Renders a parsed default expression as Snowflake SQL.
fn render_snowflake_default_expression(expression: &DefaultExpression) -> Option<String> {
    match expression {
        DefaultExpression::StringLiteral(expression)
        | DefaultExpression::NumericLiteral(expression)
        | DefaultExpression::BooleanLiteral(expression)
        | DefaultExpression::DateLiteral(expression)
        | DefaultExpression::TimeLiteral(expression)
        | DefaultExpression::TimestampLiteral(expression)
        | DefaultExpression::NumericExpression(expression) => Some(expression.clone()),
        DefaultExpression::JsonLiteral(expression) => Some(format!("PARSE_JSON({expression})")),
        DefaultExpression::UuidV4 => Some("UUID_STRING()".to_owned()),
        DefaultExpression::CurrentUser => Some("CURRENT_USER()".to_owned()),
        DefaultExpression::CurrentTimestamp
        | DefaultExpression::LocalTimestamp
        | DefaultExpression::TimezoneNow => Some("CURRENT_TIMESTAMP()".to_owned()),
        DefaultExpression::CurrentDate => Some("CURRENT_DATE()".to_owned()),
        DefaultExpression::CurrentTime => Some("CURRENT_TIME()".to_owned()),
        DefaultExpression::IntervalArithmetic { base, operator, interval, .. } => {
            let base = render_snowflake_default_expression(base)?;
            Some(format!("{base} {} INTERVAL '{}'", operator.as_sql(), interval.literal))
        }
        DefaultExpression::LiteralFunction { function, argument } => {
            Some(format!("{}({argument})", function.as_upper_name()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> ColumnSchema {
        ColumnSchema::new(name.to_owned(), Type::INT4, -1, 1, None, true)
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
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, None, true),
            ColumnSchema::new("created_at".to_owned(), Type::TIMESTAMPTZ, -1, 2, None, true),
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
            ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 1, None, true)
                .with_default_expression(Some("'pending'::text".to_owned())),
            ColumnSchema::new("payload".to_owned(), Type::JSONB, -1, 2, None, true)
                .with_default_expression(Some("'{}'::jsonb".to_owned())),
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
            (Type::UUID, "gen_random_uuid()", " DEFAULT UUID_STRING()"),
            (Type::TEXT, "CURRENT_USER", " DEFAULT CURRENT_USER()"),
            (
                Type::TIMESTAMPTZ,
                "now() + interval '30 days'",
                " DEFAULT CURRENT_TIMESTAMP() + INTERVAL '30 days'",
            ),
            (Type::TEXT, "upper('user'::text)", " DEFAULT UPPER('user')"),
        ];

        for (typ, expression, expected) in cases {
            let column = ColumnSchema::new("value".to_owned(), typ, -1, 1, None, true)
                .with_default_expression(Some(expression.to_owned()));

            assert_eq!(default_clause(&column).as_deref(), Some(expected));
        }
    }
}
