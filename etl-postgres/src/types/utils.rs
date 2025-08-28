use tokio_postgres::types::{Kind, Type};

/// Converts a Postgres type OID to a [`Type`] instance.
///
/// Returns a properly constructed [`Type`] for the given OID, or creates an unnamed
/// type as fallback if the OID lookup fails.
pub fn convert_type_oid_to_type(type_oid: u32) -> Type {
    Type::from_oid(type_oid).unwrap_or(Type::new(
        format!("unnamed_type({type_oid})"),
        type_oid,
        Kind::Simple,
        "pg_catalog".to_string(),
    ))
}

/// Returns whether the Postgres type is an array type.
pub fn is_array_type(typ: &Type) -> bool {
    matches!(
        typ,
        &Type::BOOL_ARRAY
            | &Type::CHAR_ARRAY
            | &Type::BPCHAR_ARRAY
            | &Type::VARCHAR_ARRAY
            | &Type::NAME_ARRAY
            | &Type::TEXT_ARRAY
            | &Type::INT2_ARRAY
            | &Type::INT4_ARRAY
            | &Type::INT8_ARRAY
            | &Type::FLOAT4_ARRAY
            | &Type::FLOAT8_ARRAY
            | &Type::NUMERIC_ARRAY
            | &Type::DATE_ARRAY
            | &Type::TIME_ARRAY
            | &Type::TIMESTAMP_ARRAY
            | &Type::TIMESTAMPTZ_ARRAY
            | &Type::UUID_ARRAY
            | &Type::JSON_ARRAY
            | &Type::JSONB_ARRAY
            | &Type::OID_ARRAY
            | &Type::BYTEA_ARRAY
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_is_array_type() {
        // array types
        assert!(is_array_type(&Type::BOOL_ARRAY));
        assert!(is_array_type(&Type::CHAR_ARRAY));
        assert!(is_array_type(&Type::BPCHAR_ARRAY));
        assert!(is_array_type(&Type::VARCHAR_ARRAY));
        assert!(is_array_type(&Type::NAME_ARRAY));
        assert!(is_array_type(&Type::TEXT_ARRAY));
        assert!(is_array_type(&Type::INT2_ARRAY));
        assert!(is_array_type(&Type::INT4_ARRAY));
        assert!(is_array_type(&Type::INT8_ARRAY));
        assert!(is_array_type(&Type::FLOAT4_ARRAY));
        assert!(is_array_type(&Type::FLOAT8_ARRAY));
        assert!(is_array_type(&Type::NUMERIC_ARRAY));
        assert!(is_array_type(&Type::DATE_ARRAY));
        assert!(is_array_type(&Type::TIME_ARRAY));
        assert!(is_array_type(&Type::TIMESTAMP_ARRAY));
        assert!(is_array_type(&Type::TIMESTAMPTZ_ARRAY));
        assert!(is_array_type(&Type::UUID_ARRAY));
        assert!(is_array_type(&Type::JSON_ARRAY));
        assert!(is_array_type(&Type::JSONB_ARRAY));
        assert!(is_array_type(&Type::OID_ARRAY));
        assert!(is_array_type(&Type::BYTEA_ARRAY));

        // scalar types
        assert!(!is_array_type(&Type::BOOL));
        assert!(!is_array_type(&Type::CHAR));
        assert!(!is_array_type(&Type::BPCHAR));
        assert!(!is_array_type(&Type::VARCHAR));
        assert!(!is_array_type(&Type::NAME));
        assert!(!is_array_type(&Type::TEXT));
        assert!(!is_array_type(&Type::INT2));
        assert!(!is_array_type(&Type::INT4));
        assert!(!is_array_type(&Type::INT8));
        assert!(!is_array_type(&Type::FLOAT4));
        assert!(!is_array_type(&Type::FLOAT8));
        assert!(!is_array_type(&Type::NUMERIC));
        assert!(!is_array_type(&Type::DATE));
        assert!(!is_array_type(&Type::TIME));
        assert!(!is_array_type(&Type::TIMESTAMP));
        assert!(!is_array_type(&Type::TIMESTAMPTZ));
        assert!(!is_array_type(&Type::UUID));
        assert!(!is_array_type(&Type::JSON));
        assert!(!is_array_type(&Type::JSONB));
        assert!(!is_array_type(&Type::OID));
        assert!(!is_array_type(&Type::BYTEA));
    }
}
