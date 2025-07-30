use tokio_postgres::types::{Kind, Type};

/// Converts a type oid to a [`Type`] defaulting to an unnamed type in case of failure to
/// look up the type.
pub fn convert_type_oid_to_type(type_oid: u32) -> Type {
    Type::from_oid(type_oid).unwrap_or(Type::new(
        format!("unnamed_type({type_oid})"),
        type_oid,
        Kind::Simple,
        "pg_catalog".to_string(),
    ))
}
