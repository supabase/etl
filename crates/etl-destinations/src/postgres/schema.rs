//! DDL generation for the Postgres destination.

use std::borrow::Cow;

use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    schema::{
        ColumnModification, ColumnSchema, DefaultExpression, NumericModifiers,
        ReplicatedTableSchema, SchemaDiff, Type, is_array_type, numeric_modifiers,
        parse_default_expression,
    },
};
use tracing::warn;

use crate::postgres::sql::{quote_identifier, quote_table_name};

/// Postgres VARHDRSZ used in typmod encoding for variable-length types.
const VARHDRSZ: i32 = 4;

/// Builds `CREATE SCHEMA IF NOT EXISTS` for a schema name.
pub(crate) fn create_schema_sql(schema_name: &str) -> String {
    format!("create schema if not exists {}", quote_identifier(schema_name))
}

/// Builds `CREATE TABLE IF NOT EXISTS` for a replicated schema.
pub(crate) fn create_table_sql(
    destination_table: &etl::schema::TableName,
    schema: &ReplicatedTableSchema,
) -> EtlResult<String> {
    ensure_has_primary_key(schema)?;

    let mut column_defs = Vec::new();
    for column in schema.column_schemas() {
        column_defs.push(column_definition(column, false, true, true));
    }

    let mut pk_columns: Vec<_> = schema.primary_key_column_schemas().collect();
    pk_columns.sort_by_key(|column| column.primary_key_ordinal_position);
    let pk_list = pk_columns
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");

    Ok(format!(
        "create table if not exists {} ({}, primary key ({}))",
        quote_table_name(destination_table),
        column_defs.join(", "),
        pk_list
    ))
}

/// Builds DDL statements that apply a [`SchemaDiff`] to an existing table.
///
/// Statement order is intentional for name-reuse collisions such as renaming
/// column `a` to `b` while also adding a new column named `a`:
/// 1. DROP removed columns
/// 2. RENAME columns
/// 3. ADD new columns
/// 4. nullability / default modifications
pub(crate) fn schema_diff_statements(
    destination_table: &etl::schema::TableName,
    diff: &SchemaDiff,
) -> Vec<String> {
    if diff.is_empty() {
        return Vec::new();
    }

    let table = quote_table_name(destination_table);
    let mut statements = Vec::new();

    for column in &diff.columns_to_remove {
        statements.push(format!(
            "alter table {table} drop column if exists {}",
            quote_identifier(&column.name)
        ));
    }

    for change in &diff.columns_to_change {
        for modification in &change.modifications {
            let ColumnModification::Rename { old_name, new_name } = modification else {
                continue;
            };
            statements.push(format!(
                "alter table {table} rename column {} to {}",
                quote_identifier(old_name),
                quote_identifier(new_name)
            ));
        }
    }

    for column in &diff.columns_to_add {
        // Existing rows cannot satisfy a new NOT NULL without a default, so force
        // nullability unless a supported default expression is present.
        let force_nullable = column.default_expression.is_none()
            || !supports_column_default(
                column.default_expression.as_deref().unwrap_or_default(),
                &column.typ,
            );
        statements.push(format!(
            "alter table {table} add column {}",
            column_definition(column, force_nullable, true, true)
        ));
    }

    for change in &diff.columns_to_change {
        for modification in &change.modifications {
            match modification {
                ColumnModification::Rename { .. } => {}
                ColumnModification::Nullability { old_nullable, new_nullable } => {
                    if !*old_nullable && *new_nullable {
                        statements.push(format!(
                            "alter table {table} alter column {} drop not null",
                            quote_identifier(&change.new_column.name)
                        ));
                    } else if *old_nullable && !*new_nullable {
                        warn!(
                            table_name = %table,
                            column_name = %change.new_column.name,
                            "skipping source column set not null for Postgres destination"
                        );
                    }
                }
                ColumnModification::Default { old_expression, new_expression } => {
                    let old_default_was_supported =
                        old_expression.as_deref().is_some_and(|expression| {
                            supports_column_default(expression, &change.old_column.typ)
                        });

                    if let Some(new_default_expression) = new_expression.as_deref() {
                        if let Some(rendered) = postgres_default_expression(
                            new_default_expression,
                            &change.new_column.typ,
                        ) {
                            statements.push(format!(
                                "alter table {table} alter column {} set default {rendered}",
                                quote_identifier(&change.new_column.name)
                            ));
                        } else {
                            warn!(
                                table_name = %table,
                                column_name = %change.new_column.name,
                                "skipping unsupported source column default for Postgres destination"
                            );
                            if old_default_was_supported {
                                statements.push(format!(
                                    "alter table {table} alter column {} drop default",
                                    quote_identifier(&change.new_column.name)
                                ));
                            }
                        }
                    } else if old_default_was_supported {
                        statements.push(format!(
                            "alter table {table} alter column {} drop default",
                            quote_identifier(&change.new_column.name)
                        ));
                    } else if old_expression.is_some() {
                        warn!(
                            table_name = %table,
                            column_name = %change.new_column.name,
                            "skipping source column default removal for Postgres destination                              because no supported destination default was set"
                        );
                    }
                }
            }
        }
    }

    statements
}

/// Builds `DROP TABLE IF EXISTS` for a destination table.
pub(crate) fn drop_table_sql(destination_table: &etl::schema::TableName) -> String {
    format!("drop table if exists {}", quote_table_name(destination_table))
}

/// Builds `TRUNCATE TABLE` for a destination table.
pub(crate) fn truncate_table_sql(destination_table: &etl::schema::TableName) -> String {
    format!("truncate table {}", quote_table_name(destination_table))
}

/// Builds an upsert statement for the given schema columns.
pub(crate) fn upsert_sql(
    destination_table: &etl::schema::TableName,
    schema: &ReplicatedTableSchema,
) -> EtlResult<String> {
    ensure_has_primary_key(schema)?;

    let columns: Vec<_> = schema.column_schemas().collect();
    let column_list =
        columns.iter().map(|column| quote_identifier(&column.name)).collect::<Vec<_>>().join(", ");
    let placeholders =
        (1..=columns.len()).map(|index| format!("${index}")).collect::<Vec<_>>().join(", ");

    let mut pk_columns: Vec<_> = schema.primary_key_column_schemas().collect();
    pk_columns.sort_by_key(|column| column.primary_key_ordinal_position);
    let pk_list = pk_columns
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");

    let update_assignments = columns
        .iter()
        .filter(|column| column.primary_key_ordinal_position.is_none())
        .map(|column| {
            let name = quote_identifier(&column.name);
            format!("{name} = excluded.{name}")
        })
        .collect::<Vec<_>>();

    if update_assignments.is_empty() {
        Ok(format!(
            "insert into {} ({column_list}) values ({placeholders}) on conflict ({pk_list}) do \
             nothing",
            quote_table_name(destination_table)
        ))
    } else {
        Ok(format!(
            "insert into {} ({column_list}) values ({placeholders}) on conflict ({pk_list}) do \
             update set {}",
            quote_table_name(destination_table),
            update_assignments.join(", ")
        ))
    }
}

/// Builds a delete-by-primary-key statement.
pub(crate) fn delete_by_pk_sql(
    destination_table: &etl::schema::TableName,
    schema: &ReplicatedTableSchema,
) -> EtlResult<String> {
    ensure_has_primary_key(schema)?;

    let pk_columns: Vec<_> = schema.primary_key_column_schemas().collect();
    let predicates = pk_columns
        .iter()
        .enumerate()
        .map(|(index, column)| format!("{} = ${}", quote_identifier(&column.name), index + 1))
        .collect::<Vec<_>>()
        .join(" and ");

    Ok(format!("delete from {} where {predicates}", quote_table_name(destination_table)))
}

/// Returns whether a table has at least one replicated primary-key column.
pub(crate) fn ensure_has_primary_key(schema: &ReplicatedTableSchema) -> EtlResult<()> {
    if schema.primary_key_column_schemas().len() == 0 {
        return Err(etl_error!(
            ErrorKind::ValidationError,
            "Postgres destination requires a primary key",
            format!(
                "Table '{}' has no replicated primary-key columns. The Postgres destination uses \
                 UPSERT tables keyed by the source primary key.",
                schema.name()
            )
        ));
    }

    if !schema.all_primary_key_columns_replicated() {
        let missing = schema
            .unreplicated_primary_key_column_schemas()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(etl_error!(
            ErrorKind::ValidationError,
            "Postgres destination requires all primary-key columns to be replicated",
            format!(
                "Table '{}' omits primary-key columns from replication: {missing}",
                schema.name()
            )
        ));
    }

    Ok(())
}

/// Builds one column definition for CREATE/ALTER TABLE.
fn column_definition(
    column: &ColumnSchema,
    force_nullable: bool,
    include_default: bool,
    include_not_null: bool,
) -> String {
    let sql_type = postgres_column_type_sql(&column.typ, column.modifier);
    let default_clause = if include_default {
        column
            .default_expression
            .as_deref()
            .and_then(|expression| postgres_default_expression(expression, &column.typ))
            .map(|rendered| format!(" default {rendered}"))
            .unwrap_or_default()
    } else {
        String::new()
    };
    let nullable = force_nullable || column.nullable || !include_not_null;
    let nullability = if nullable { "" } else { " not null" };
    format!("{} {sql_type}{default_clause}{nullability}", quote_identifier(&column.name))
}

/// Returns the Postgres SQL type for a column type and modifier.
pub(crate) fn postgres_column_type_sql(typ: &Type, modifier: i32) -> Cow<'static, str> {
    if is_array_type(typ) {
        let element = array_element_type_sql(typ, modifier);
        format!("{element}[]").into()
    } else {
        scalar_type_sql(typ, modifier)
    }
}

fn scalar_type_sql(typ: &Type, modifier: i32) -> Cow<'static, str> {
    match *typ {
        Type::BOOL => "boolean".into(),
        Type::INT2 => "smallint".into(),
        Type::INT4 => "integer".into(),
        Type::INT8 => "bigint".into(),
        Type::FLOAT4 => "real".into(),
        Type::FLOAT8 => "double precision".into(),
        Type::NUMERIC => match numeric_modifiers(modifier) {
            Some(NumericModifiers { p, s }) if s >= 0 => format!("numeric({p}, {s})").into(),
            Some(NumericModifiers { p, .. }) => format!("numeric({p})").into(),
            None => "numeric".into(),
        },
        Type::TEXT | Type::NAME => "text".into(),
        Type::VARCHAR => match char_length(modifier) {
            Some(length) => format!("varchar({length})").into(),
            None => "varchar".into(),
        },
        Type::BPCHAR | Type::CHAR => match char_length(modifier) {
            Some(length) => format!("character({length})").into(),
            None => "character".into(),
        },
        Type::DATE => "date".into(),
        Type::TIME => "time".into(),
        // Bound as text via PostgresValue; keep the destination column as text so
        // prepared-statement parameter types match the encoded values.
        Type::TIMETZ => "text".into(),
        Type::TIMESTAMP => "timestamp".into(),
        Type::TIMESTAMPTZ => "timestamptz".into(),
        Type::UUID => "uuid".into(),
        Type::JSON => "json".into(),
        Type::JSONB => "jsonb".into(),
        Type::BYTEA => "bytea".into(),
        Type::OID => "oid".into(),
        Type::INET => "inet".into(),
        Type::CIDR => "cidr".into(),
        Type::MACADDR => "macaddr".into(),
        Type::INTERVAL => "interval".into(),
        _ => "text".into(),
    }
}

fn array_element_type_sql(typ: &Type, modifier: i32) -> Cow<'static, str> {
    match *typ {
        Type::BOOL_ARRAY => "boolean".into(),
        Type::INT2_ARRAY => "smallint".into(),
        Type::INT4_ARRAY => "integer".into(),
        Type::INT8_ARRAY => "bigint".into(),
        Type::FLOAT4_ARRAY => "real".into(),
        Type::FLOAT8_ARRAY => "double precision".into(),
        Type::NUMERIC_ARRAY => match numeric_modifiers(modifier) {
            Some(NumericModifiers { p, s }) if s >= 0 => format!("numeric({p}, {s})").into(),
            Some(NumericModifiers { p, .. }) => format!("numeric({p})").into(),
            None => "numeric".into(),
        },
        Type::TEXT_ARRAY => "text".into(),
        Type::VARCHAR_ARRAY => match char_length(modifier) {
            Some(length) => format!("varchar({length})").into(),
            None => "varchar".into(),
        },
        Type::BPCHAR_ARRAY => match char_length(modifier) {
            Some(length) => format!("character({length})").into(),
            None => "character".into(),
        },
        Type::DATE_ARRAY => "date".into(),
        Type::TIME_ARRAY => "time".into(),
        Type::TIMETZ_ARRAY => "text".into(),
        Type::TIMESTAMP_ARRAY => "timestamp".into(),
        Type::TIMESTAMPTZ_ARRAY => "timestamptz".into(),
        Type::UUID_ARRAY => "uuid".into(),
        Type::JSON_ARRAY => "json".into(),
        Type::JSONB_ARRAY => "jsonb".into(),
        Type::BYTEA_ARRAY => "bytea".into(),
        Type::OID_ARRAY => "oid".into(),
        Type::INET_ARRAY => "inet".into(),
        Type::CIDR_ARRAY => "cidr".into(),
        Type::MACADDR_ARRAY => "macaddr".into(),
        Type::INTERVAL_ARRAY => "interval".into(),
        _ => "text".into(),
    }
}

/// Returns VARCHAR/BPCHAR character length from typmod, if constrained.
fn char_length(modifier: i32) -> Option<i32> {
    if modifier == -1 { None } else { Some(modifier - VARHDRSZ) }
}

/// Returns whether a default expression can be applied on Postgres.
pub(crate) fn supports_column_default(default_expression: &str, typ: &Type) -> bool {
    postgres_default_expression(default_expression, typ).is_some()
}

/// Renders a supported default expression as Postgres SQL.
pub(crate) fn postgres_default_expression(default_expression: &str, typ: &Type) -> Option<String> {
    parse_default_expression(default_expression, typ).map(|expression| match expression {
        DefaultExpression::StringLiteral(value)
        | DefaultExpression::NumericLiteral(value)
        | DefaultExpression::BooleanLiteral(value)
        | DefaultExpression::DateLiteral(value)
        | DefaultExpression::TimeLiteral(value)
        | DefaultExpression::TimeTzLiteral(value)
        | DefaultExpression::TimestampLiteral(value)
        | DefaultExpression::TimestampTzLiteral(value)
        | DefaultExpression::IntervalLiteral(value)
        | DefaultExpression::JsonLiteral(value) => value,
    })
}

#[cfg(test)]
mod tests {
    use etl::schema::{
        ColumnChange, ColumnModification, ColumnSchema, SchemaDiff, TableName, Type,
    };

    use super::{postgres_column_type_sql, schema_diff_statements};

    #[test]
    fn schema_diff_statements_orders_drop_rename_add_for_name_reuse() {
        let destination_table = TableName::new("replica".to_owned(), "items".to_owned());
        let diff = SchemaDiff {
            columns_to_add: vec![ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 4, true)],
            columns_to_remove: vec![ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 3, true)],
            columns_to_change: vec![ColumnChange {
                ordinal_position: 2,
                old_column: ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
                new_column: ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 2, true),
                modifications: vec![ColumnModification::Rename {
                    old_name: "name".to_owned(),
                    new_name: "value".to_owned(),
                }],
            }],
        };

        let statements = schema_diff_statements(&destination_table, &diff);
        assert_eq!(statements.len(), 3);
        assert!(
            statements[0].contains("drop column") && statements[0].contains("value"),
            "expected drop of value first, got {:?}",
            statements[0]
        );
        assert!(
            statements[1].contains("rename column")
                && statements[1].contains("name")
                && statements[1].contains("value"),
            "expected rename name to value second, got {:?}",
            statements[1]
        );
        assert!(
            statements[2].contains("add column") && statements[2].contains("name"),
            "expected add of name third, got {:?}",
            statements[2]
        );
        assert!(!statements[2].contains("if not exists"));
    }

    #[test]
    fn postgres_column_type_sql_maps_timetz_to_text() {
        assert_eq!(postgres_column_type_sql(&Type::TIMETZ, -1), "text");
        assert_eq!(postgres_column_type_sql(&Type::TIMETZ_ARRAY, -1), "text[]");
    }
}
