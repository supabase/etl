use etl::schema::{
    ColumnSchema, DefaultExpression, IdentityType, ReplicatedTableSchema, Type, is_array_type,
    parse_default_expression, unquote_postgres_string_literal,
};
use tracing::warn;

use crate::snowflake::{
    Error, Result,
    sql::{quote_identifier, quote_string_literal},
};

/// Name of the column storing the CDC operation.
pub(crate) const CDC_OPERATION_COLUMN: &str = "_cdc_operation";
/// Name of the column storing the CDC sequence number.
pub(crate) const CDC_SEQUENCE_COLUMN: &str = "_cdc_sequence_number";
/// Name of the column listing PostgreSQL attribute numbers omitted from the CDC
/// event.
pub(super) const CDC_MISSING_COLUMNS_COLUMN: &str = "_cdc_missing_columns";

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
        if col.name == CDC_OPERATION_COLUMN
            || col.name == CDC_SEQUENCE_COLUMN
            || col.name == CDC_MISSING_COLUMNS_COLUMN
        {
            return Err(Error::Config(format!(
                "Source column '{}' collides with reserved CDC column name",
                col.name,
            )));
        }
    }
    Ok(())
}

/// Checks replica identity and primary-key requirements shared by
/// partial-update writes and materialization queries.
pub(super) fn validate_primary_key_identity(schema: &ReplicatedTableSchema) -> Result<()> {
    if !matches!(schema.identity_type(), IdentityType::PrimaryKey | IdentityType::Full)
        || !schema.column_schemas().any(ColumnSchema::primary_key)
        || !schema.all_primary_key_columns_replicated()
        || schema.inner().primary_key_deferrable
    {
        return Err(Error::UnsupportedIdentity(
            "A complete nondeferrable primary-key identity is required".to_owned(),
        ));
    }
    for column in schema.column_schemas() {
        if column.primary_key()
            && !matches!(
                &column.typ,
                &Type::INT2
                    | &Type::INT4
                    | &Type::INT8
                    | &Type::OID
                    | &Type::BOOL
                    | &Type::TEXT
                    | &Type::VARCHAR
                    | &Type::CHAR
                    | &Type::BPCHAR
                    | &Type::NAME
                    | &Type::UUID
                    | &Type::BYTEA
                    | &Type::NUMERIC
                    | &Type::DATE
                    | &Type::TIMESTAMP
                    | &Type::TIMESTAMPTZ
            )
        {
            return Err(Error::UnsupportedIdentity(format!(
                "Primary-key column '{}' has an unsupported type",
                column.name
            )));
        }
    }
    Ok(())
}

/// Builds the column definitions string.
///
/// Source columns are followed by the operation and sequence columns, and
/// the nullable missing-column mask, matching upgraded legacy tables.
pub(crate) fn build_column_defs(columns: &[ColumnSchema]) -> String {
    let mut parts: Vec<String> = columns
        .iter()
        .map(|col| {
            if !col.nullable {
                warn!(
                    column_name = %col.name,
                    "creating a source not null column as nullable in snowflake so key-only delete \
                     records remain writable; the destination schema will be more permissive"
                );
            }
            let default_clause = default_clause(col).unwrap_or_default();
            format!("{} {}{}", quote_identifier(&col.name), type_name(&col.typ), default_clause)
        })
        .collect();

    parts.push(format!("{} VARCHAR NOT NULL", quote_identifier(CDC_OPERATION_COLUMN)));
    parts.push(format!("{} VARCHAR NOT NULL", quote_identifier(CDC_SEQUENCE_COLUMN)));

    parts.push(format!("{} ARRAY", quote_identifier(CDC_MISSING_COLUMNS_COLUMN)));
    parts.join(", ")
}

/// Returns the Snowflake default clause for a column, if supported.
pub(crate) fn default_clause(column_schema: &ColumnSchema) -> Option<String> {
    // PostgreSQL already applies source defaults before replication. VARIANT
    // defaults would replace the omitted fields used to represent SQL NULL.
    if is_json_type(&column_schema.typ) {
        if column_schema.default_expression.is_some() {
            warn!(
                column_name = %column_schema.name,
                "omitting snowflake json column default to preserve sql null; postgres applies the source default before replication"
            );
        }
        return None;
    }

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
            "skipping unsupported source column default for snowflake"
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
            "skipping source column default for snowflake add column because snowflake only \
             supports literal add-column defaults"
        );
    }

    default_clause
}

/// Returns a rendered Snowflake default expression for a column, if supported.
fn snowflake_default_expression(default_expression: &str, typ: &Type) -> Option<String> {
    parse_default_expression(default_expression, typ)
        .and_then(|expression| render_snowflake_default_expression(&expression, typ))
}

/// Returns a Snowflake `ADD COLUMN` default expression, if supported.
fn snowflake_add_column_default_expression(default_expression: &str, typ: &Type) -> Option<String> {
    parse_default_expression(default_expression, typ)
        .filter(|expression| {
            matches!(
                expression,
                DefaultExpression::StringLiteral(_)
                    | DefaultExpression::NumericLiteral(_)
                    | DefaultExpression::BooleanLiteral(_)
            )
        })
        .and_then(|expression| render_snowflake_default_expression(&expression, typ))
}

/// Renders a parsed default expression as Snowflake SQL.
fn render_snowflake_default_expression(
    expression: &DefaultExpression,
    typ: &Type,
) -> Option<String> {
    match expression {
        DefaultExpression::StringLiteral(expression) if is_snowflake_string_default_type(typ) => {
            snowflake_string_literal(expression)
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
        DefaultExpression::DateLiteral(expression) if matches!(typ, &Type::DATE) => {
            snowflake_string_literal(expression).map(|literal| format!("DATE {literal}"))
        }
        DefaultExpression::TimeLiteral(expression) if matches!(typ, &Type::TIME) => {
            snowflake_string_literal(expression).map(|literal| format!("TIME {literal}"))
        }
        DefaultExpression::TimeTzLiteral(expression) if matches!(typ, &Type::TIMETZ) => {
            snowflake_string_literal(expression)
        }
        DefaultExpression::TimestampLiteral(expression) if matches!(typ, &Type::TIMESTAMP) => {
            snowflake_string_literal(expression)
                .map(|literal| format!("TO_TIMESTAMP_NTZ({literal})"))
        }
        DefaultExpression::TimestampTzLiteral(expression) if matches!(typ, &Type::TIMESTAMPTZ) => {
            snowflake_string_literal(expression)
                .map(|literal| format!("TO_TIMESTAMP_TZ({literal})"))
        }
        DefaultExpression::IntervalLiteral(expression) if matches!(typ, &Type::INTERVAL) => {
            snowflake_string_literal(expression)
        }
        _ => None,
    }
}

/// Re-quotes one parser-validated PostgreSQL literal using Snowflake escapes.
fn snowflake_string_literal(expression: &str) -> Option<String> {
    unquote_postgres_string_literal(expression).map(|value| quote_string_literal(&value))
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
pub(super) fn is_json_type(typ: &Type) -> bool {
    matches!(typ, &Type::JSON | &Type::JSONB)
}

/// Quotes a parser-validated numeric literal as a SQL string literal.
fn quote_numeric_literal_as_string(expression: &str) -> String {
    format!("'{expression}'")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use etl::schema::{IdentityMask, ReplicationMask, TableId, TableName, TableSchema};

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
            // Arrays all map to ARRAY.
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
    fn build_column_defs_keeps_source_columns_nullable() {
        let cases = vec![
            (ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false), r#""id" INTEGER"#),
            (
                ColumnSchema::new("created_at".to_owned(), Type::TIMESTAMPTZ, -1, 2, true),
                r#""created_at" TIMESTAMP_TZ"#,
            ),
            (
                ColumnSchema::new("text_col".to_owned(), Type::TEXT, -1, 3, true)
                    .with_default_expression("'base_text_literal'::text".to_owned()),
                r#""text_col" VARCHAR DEFAULT 'base_text_literal'"#,
            ),
            (
                ColumnSchema::new("varchar_col".to_owned(), Type::VARCHAR, 255, 4, true)
                    .with_default_expression(
                        "'base_varchar_literal'::character varying".to_owned(),
                    ),
                r#""varchar_col" VARCHAR DEFAULT 'base_varchar_literal'"#,
            ),
            (
                ColumnSchema::new("smallint_col".to_owned(), Type::INT2, -1, 5, true)
                    .with_default_expression("7".to_owned()),
                r#""smallint_col" SMALLINT DEFAULT 7"#,
            ),
            (
                ColumnSchema::new("integer_col".to_owned(), Type::INT4, -1, 6, true)
                    .with_default_expression("42".to_owned()),
                r#""integer_col" INTEGER DEFAULT 42"#,
            ),
            (
                ColumnSchema::new("numeric_col".to_owned(), Type::NUMERIC, -1, 7, true)
                    .with_default_expression("(10 + 5)".to_owned()),
                r#""numeric_col" VARCHAR"#,
            ),
            (
                ColumnSchema::new("boolean_col".to_owned(), Type::BOOL, -1, 8, true)
                    .with_default_expression("false".to_owned()),
                r#""boolean_col" BOOLEAN DEFAULT false"#,
            ),
            (
                ColumnSchema::new("date_col".to_owned(), Type::DATE, -1, 9, true)
                    .with_default_expression("'2026-01-01'::date".to_owned()),
                r#""date_col" DATE DEFAULT DATE '2026-01-01'"#,
            ),
            (
                ColumnSchema::new("timestamp_col".to_owned(), Type::TIMESTAMP, -1, 10, true)
                    .with_default_expression("'2026-01-01 12:30:00'::timestamp".to_owned()),
                r#""timestamp_col" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ('2026-01-01 12:30:00')"#,
            ),
            (
                ColumnSchema::new("timestamptz_col".to_owned(), Type::TIMESTAMPTZ, -1, 11, true)
                    .with_default_expression("now()".to_owned()),
                r#""timestamptz_col" TIMESTAMP_TZ"#,
            ),
            (
                ColumnSchema::new("time_col".to_owned(), Type::TIME, -1, 12, true)
                    .with_default_expression("'12:30:00'::time".to_owned()),
                r#""time_col" TIME DEFAULT TIME '12:30:00'"#,
            ),
            (
                ColumnSchema::new("jsonb_col".to_owned(), Type::JSONB, -1, 13, true)
                    .with_default_expression(r#"'{"source": "base"}'::jsonb"#.to_owned()),
                r#""jsonb_col" VARIANT"#,
            ),
            (
                ColumnSchema::new("json_col".to_owned(), Type::JSON, -1, 14, true)
                    .with_default_expression(r#"'{"source": "base"}'::json"#.to_owned()),
                r#""json_col" VARIANT"#,
            ),
        ];
        let (columns, mut expected): (Vec<_>, Vec<_>) = cases.into_iter().unzip();
        expected.extend([
            r#""_cdc_operation" VARCHAR NOT NULL"#,
            r#""_cdc_sequence_number" VARCHAR NOT NULL"#,
            r#""_cdc_missing_columns" ARRAY"#,
        ]);

        let defs = build_column_defs(&columns);

        assert_eq!(defs, expected.join(", "));
    }

    #[test]
    fn default_clause_renders_portable_expressions() {
        let cases = [
            (Type::TEXT, "true", " DEFAULT 'true'"),
            (Type::BOOL, "'true'::text", " DEFAULT true"),
            (Type::NUMERIC, "42", " DEFAULT '42'"),
            (Type::DATE, "'2026-01-01'::date", " DEFAULT DATE '2026-01-01'"),
            (Type::TIME, "'12:30:00'::time", " DEFAULT TIME '12:30:00'"),
            (Type::TIMETZ, "'12:30:00+02'::timetz", " DEFAULT '12:30:00+02'"),
            (Type::INTERVAL, "'30 days'::interval", " DEFAULT '30 days'"),
            (
                Type::TIMESTAMP,
                "'2026-01-01 12:30:00'::timestamp",
                " DEFAULT TO_TIMESTAMP_NTZ('2026-01-01 12:30:00')",
            ),
            (
                Type::TIMESTAMPTZ,
                "'2026-01-01 12:30:00'::timestamptz",
                " DEFAULT TO_TIMESTAMP_TZ('2026-01-01 12:30:00')",
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
    fn default_clause_requotes_postgres_backslashes_for_snowflake() {
        let text_column = ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 1, true)
            .with_default_expression(r#"'path\d'::text"#.to_owned());
        let json_column = ColumnSchema::new("payload".to_owned(), Type::JSONB, -1, 2, true)
            .with_default_expression(r#"'{"pattern":"\d"}'::jsonb"#.to_owned());

        assert_eq!(default_clause(&text_column).as_deref(), Some(r#" DEFAULT 'path\\d'"#));
        assert_eq!(default_clause(&json_column), None);
    }

    #[test]
    fn add_column_default_clause_renders_supported_expressions() {
        let cases = [
            (Type::TEXT, "'pending'::text", Some(" DEFAULT 'pending'")),
            (Type::INT4, "15", Some(" DEFAULT 15")),
            (Type::BOOL, "true", Some(" DEFAULT true")),
            (Type::NUMERIC, "42", Some(" DEFAULT '42'")),
            (
                Type::UUID,
                "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid",
                Some(" DEFAULT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"),
            ),
            (Type::DATE, "'2026-01-01'::date", None),
            (Type::TIME, "'12:30:00'::time", None),
            (Type::TIMETZ, "'12:30:00+02'::timetz", None),
            (Type::TIMESTAMP, "'2026-01-01 12:30:00'::timestamp", None),
            (Type::TIMESTAMPTZ, "'2026-01-01 12:30:00+02'::timestamptz", None),
            (Type::INTERVAL, "'30 days'::interval", None),
            (Type::JSONB, "'{}'::jsonb", None),
            (Type::UUID, "gen_random_uuid()", None),
            (Type::INT4, "10 + 5", None),
        ];

        for (typ, expression, expected) in cases {
            let column = ColumnSchema::new("value".to_owned(), typ, -1, 1, true)
                .with_default_expression(expression.to_owned());
            let add_clause = add_column_default_clause(&column);

            assert_eq!(add_clause.as_deref(), expected, "add-column default for {expression}");
            if add_clause.is_some() {
                assert_eq!(add_clause, default_clause(&column));
            }
        }
    }

    /// Requires complete nondeferrable keys with supported types for partial
    /// updates.
    #[test]
    fn supported_primary_key_identities() {
        let schema = ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".into(), "events".into()),
            vec![
                ColumnSchema::new("id".into(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("payload".into(), Type::TEXT, -1, 2, true),
            ],
        )));
        validate_primary_key_identity(&schema).unwrap();
        let mut table = schema.inner().clone();
        table.primary_key_deferrable = true;
        assert!(
            validate_primary_key_identity(&ReplicatedTableSchema::all(Arc::new(table))).is_err()
        );
        let full = ReplicatedTableSchema::from_masks(
            Arc::new(schema.inner().clone()),
            schema.replication_mask().clone(),
            IdentityMask::from_bytes(vec![1, 1]),
        );
        validate_primary_key_identity(&full).unwrap();
        let omitted_key = ReplicatedTableSchema::from_mask(
            Arc::new(schema.inner().clone()),
            ReplicationMask::from_bytes(vec![0, 1]),
        );
        assert!(validate_primary_key_identity(&omitted_key).is_err());
        for typ in [Type::INT4_ARRAY, Type::JSONB, Type::FLOAT8, Type::TIME, Type::INTERVAL] {
            let mut table = schema.inner().clone();
            table.column_schemas[0].typ = typ;
            assert!(
                validate_primary_key_identity(&ReplicatedTableSchema::all(Arc::new(table)))
                    .is_err()
            );
        }
    }
}
