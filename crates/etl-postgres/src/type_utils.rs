use tokio_postgres::types::{Kind, Type};

/// Converts a Postgres type OID to a [`Type`] instance.
///
/// Returns a properly constructed [`Type`] for the given OID, or return TEXT
/// type as fallback if the OID lookup fails.
pub fn convert_type_oid_to_type(type_oid: u32) -> Type {
    Type::from_oid(type_oid).unwrap_or(Type::TEXT)
}

/// Returns whether the Postgres type is an array type.
pub fn is_array_type(typ: &Type) -> bool {
    // `int2vector` and `oidvector` have array kind, but they are not regular
    // PostgreSQL array types and do not use underscore-prefixed array names.
    matches!(typ.kind(), Kind::Array(_)) && typ.name().starts_with('_')
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn is_array_type_fn() {
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
        assert!(is_array_type(&Type::MONEY_ARRAY));
        assert!(is_array_type(&Type::DATE_ARRAY));
        assert!(is_array_type(&Type::TIME_ARRAY));
        assert!(is_array_type(&Type::TIMESTAMP_ARRAY));
        assert!(is_array_type(&Type::TIMESTAMPTZ_ARRAY));
        assert!(is_array_type(&Type::UUID_ARRAY));
        assert!(is_array_type(&Type::JSON_ARRAY));
        assert!(is_array_type(&Type::JSONB_ARRAY));
        assert!(is_array_type(&Type::OID_ARRAY));
        assert!(is_array_type(&Type::BYTEA_ARRAY));
        assert!(is_array_type(&Type::INTERVAL_ARRAY));
        assert!(is_array_type(&Type::TIMETZ_ARRAY));
        assert!(is_array_type(&Type::INET_ARRAY));
        assert!(is_array_type(&Type::CIDR_ARRAY));
        assert!(is_array_type(&Type::MACADDR_ARRAY));
        assert!(is_array_type(&Type::MACADDR8_ARRAY));
        assert!(is_array_type(&Type::XML_ARRAY));
        assert!(is_array_type(&Type::INT4_RANGE_ARRAY));
        assert!(is_array_type(&Type::NUMMULTI_RANGE_ARRAY));
        assert!(is_array_type(&Type::INT2_VECTOR_ARRAY));

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
        assert!(!is_array_type(&Type::MONEY));
        assert!(!is_array_type(&Type::DATE));
        assert!(!is_array_type(&Type::TIME));
        assert!(!is_array_type(&Type::TIMESTAMP));
        assert!(!is_array_type(&Type::TIMESTAMPTZ));
        assert!(!is_array_type(&Type::UUID));
        assert!(!is_array_type(&Type::JSON));
        assert!(!is_array_type(&Type::JSONB));
        assert!(!is_array_type(&Type::OID));
        assert!(!is_array_type(&Type::BYTEA));
        assert!(!is_array_type(&Type::INTERVAL));
        assert!(!is_array_type(&Type::TIMETZ));
        assert!(!is_array_type(&Type::INET));
        assert!(!is_array_type(&Type::CIDR));
        assert!(!is_array_type(&Type::MACADDR));
        assert!(!is_array_type(&Type::MACADDR8));
        assert!(!is_array_type(&Type::XML));
        assert!(!is_array_type(&Type::INT4_RANGE));
        assert!(!is_array_type(&Type::NUMMULTI_RANGE));
        assert!(!is_array_type(&Type::INT2_VECTOR));
        assert!(!is_array_type(&Type::OID_VECTOR));
        assert!(!is_array_type(&Type::ANYARRAY));
    }
}
