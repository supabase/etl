use std::fmt::Formatter;
use std::str::FromStr;
use std::{error, fmt};
use tokio_postgres::types::{Kind, PgLsn, Type};

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

#[derive(Debug)]
pub struct InnerParseLsnError;

impl fmt::Display for InnerParseLsnError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "invalid postgres lsn")
    }
}

impl error::Error for InnerParseLsnError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

/// New type struct that implements the `Error` trait when parsing [`PgLsn`].
///
/// This type is a temporary measure until the `Error` trait will be implemented in the main
/// crate hosting [`PgLsn`].
pub struct InnerPgLsn(PgLsn);

impl From<PgLsn> for InnerPgLsn {
    fn from(lsn: PgLsn) -> InnerPgLsn {
        InnerPgLsn(lsn)
    }
}

impl From<InnerPgLsn> for PgLsn {
    fn from(lsn: InnerPgLsn) -> PgLsn {
        lsn.0
    }
}

impl FromStr for InnerPgLsn {
    type Err = InnerParseLsnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = PgLsn::from_str(s).map_err(|_| InnerParseLsnError)?;

        Ok(InnerPgLsn(value))
    }
}
