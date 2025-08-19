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
