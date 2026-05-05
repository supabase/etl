use etl::types::{ColumnSchema, Type, is_array_type};

use crate::snowflake::{Error, Result, sql_client::quote_identifier};

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
        .map(|col| format!("{} {}", quote_identifier(&col.name), type_name(&col.typ),))
        .collect();

    parts.push(format!("{} VARCHAR NOT NULL", quote_identifier(CDC_OPERATION_COLUMN)));
    parts.push(format!("{} VARCHAR NOT NULL", quote_identifier(CDC_SEQUENCE_COLUMN)));

    parts.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> ColumnSchema {
        ColumnSchema::new(name.to_string(), Type::INT4, -1, 1, None, true)
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
            ColumnSchema::new("id".to_string(), Type::INT4, -1, 1, None, true),
            ColumnSchema::new("created_at".to_string(), Type::TIMESTAMPTZ, -1, 2, None, true),
        ];
        let defs = build_column_defs(&columns);
        assert_eq!(
            defs,
            r#""id" INTEGER, "created_at" TIMESTAMP_TZ, "_cdc_operation" VARCHAR NOT NULL, "_cdc_sequence_number" VARCHAR NOT NULL"#
        );
    }
}
