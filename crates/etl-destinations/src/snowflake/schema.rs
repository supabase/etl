use etl::types::{ColumnSchema, ReplicatedTableSchema, Type, is_array_type};

use crate::snowflake::{Error, Result, sql::quote_identifier};

pub(crate) const CDC_OPERATION_COLUMN: &str = "_cdc_operation";
pub(crate) const CDC_SEQUENCE_COLUMN: &str = "_cdc_sequence_number";

/// Returns the Snowflake DDL type string for a given Postgres type.
///
/// Array types map to ARRAY (Snowflake's native array type, a subtype of
/// VARIANT). Scalar types use lossless Snowflake equivalents, falling back to
/// VARCHAR when Snowflake's native type cannot cover PostgreSQL's value range.
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

/// Rejects same-source-column type changes that Snowflake cannot apply safely.
pub(crate) fn validate_no_type_changes(
    current_schema: &ReplicatedTableSchema,
    new_schema: &ReplicatedTableSchema,
) -> Result<()> {
    for current_column in current_schema.column_schemas() {
        let Some(new_column) = new_schema
            .column_schemas()
            .find(|column| column.ordinal_position == current_column.ordinal_position)
        else {
            continue;
        };

        let current_type_name = type_name(&current_column.typ);
        let new_type_name = type_name(&new_column.typ);
        if current_type_name != new_type_name {
            return Err(Error::Config(format!(
                "Source column '{}' changed Snowflake type from {} to {}, which Snowflake \
                 destination schema changes do not support",
                current_column.name, current_type_name, new_type_name
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
        .map(|col| format!("{} {}", quote_identifier(&col.name), type_name(&col.typ),))
        .collect();

    parts.push(format!("{} VARCHAR NOT NULL", quote_identifier(CDC_OPERATION_COLUMN)));
    parts.push(format!("{} VARCHAR NOT NULL", quote_identifier(CDC_SEQUENCE_COLUMN)));

    parts.join(", ")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use etl::types::{ReplicationMask, TableId, TableName, TableSchema};

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

    fn replicated_schema(columns: Vec<ColumnSchema>) -> ReplicatedTableSchema {
        let schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            columns,
        ));
        let mask = ReplicationMask::all(&schema);

        ReplicatedTableSchema::from_mask(schema, mask)
    }

    #[test]
    fn type_change_validation_allows_same_types_and_renames() {
        let current_schema = replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, Some(1), false),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, None, true),
        ]);
        let new_schema = replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, Some(1), false),
            ColumnSchema::new("full_name".to_owned(), Type::TEXT, -1, 2, None, true),
        ]);

        validate_no_type_changes(&current_schema, &new_schema).unwrap();
    }

    #[test]
    fn type_change_validation_allows_same_snowflake_type_changes() {
        let current_schema = replicated_schema(vec![
            ColumnSchema::new("name".to_owned(), Type::VARCHAR, -1, 1, None, true),
            ColumnSchema::new("payload".to_owned(), Type::JSON, -1, 2, None, true),
            ColumnSchema::new("items".to_owned(), Type::INT4_ARRAY, -1, 3, None, true),
        ]);
        let new_schema = replicated_schema(vec![
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 1, None, true),
            ColumnSchema::new("payload".to_owned(), Type::JSONB, -1, 2, None, true),
            ColumnSchema::new("items".to_owned(), Type::TEXT_ARRAY, -1, 3, None, true),
        ]);

        validate_no_type_changes(&current_schema, &new_schema).unwrap();
    }

    #[test]
    fn type_change_validation_rejects_same_ordinal_type_changes() {
        let current_schema = replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, Some(1), false),
            ColumnSchema::new("score".to_owned(), Type::INT4, -1, 2, None, true),
        ]);
        let new_schema = replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, Some(1), false),
            ColumnSchema::new("score".to_owned(), Type::TEXT, -1, 2, None, true),
        ]);

        let error = validate_no_type_changes(&current_schema, &new_schema).unwrap_err();

        assert!(
            matches!(error, Error::Config(message) if message.contains("changed Snowflake type"))
        );
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
}
