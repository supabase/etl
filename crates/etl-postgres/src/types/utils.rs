use tokio_postgres::types::{Kind, PgLsn, Type};

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

/// Creates a hex-encoded sequence number from Postgres LSNs to ensure correct
/// event ordering.
///
/// Creates a hex-encoded sequence number that ensures events are processed in
/// the correct order even when they have the same system time. The format is
/// compatible with BigQuery's `_CHANGE_SEQUENCE_NUMBER` column requirements.
///
/// The rationale for using the LSN is that downstream systems will preserve the
/// highest sequence number in case of equal primary key, which is what we want
/// since in case of updates, we want the latest update in Postgres order to be
/// the winner. We have first the `commit_lsn` in the key so that operations are
/// first ordered based on the LSN at which the transaction committed,
/// and if two operations belong to the same transaction (meaning they have the
/// same `commit_lsn`), the `start_lsn` will be used as a tiebreaker. We first
/// order by `commit_lsn` to preserve the order in which operations are received
/// by the pipeline since transactions are ordered by commit time
/// and not interleaved.
pub fn generate_sequence_number(start_lsn: PgLsn, commit_lsn: PgLsn) -> String {
    let start_lsn = u64::from(start_lsn);
    let commit_lsn = u64::from(commit_lsn);

    format!("{commit_lsn:016x}/{start_lsn:016x}")
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

    #[test]
    fn generate_sequence_number_fn() {
        assert_eq!(
            generate_sequence_number(PgLsn::from(0), PgLsn::from(0)),
            "0000000000000000/0000000000000000"
        );
        assert_eq!(
            generate_sequence_number(PgLsn::from(1), PgLsn::from(0)),
            "0000000000000000/0000000000000001"
        );
        assert_eq!(
            generate_sequence_number(PgLsn::from(255), PgLsn::from(0)),
            "0000000000000000/00000000000000ff"
        );
        assert_eq!(
            generate_sequence_number(PgLsn::from(65535), PgLsn::from(0)),
            "0000000000000000/000000000000ffff"
        );
        assert_eq!(
            generate_sequence_number(PgLsn::from(u64::MAX), PgLsn::from(0)),
            "0000000000000000/ffffffffffffffff"
        );
    }
}
