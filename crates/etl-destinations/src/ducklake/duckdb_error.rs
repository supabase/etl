//! Retry classification of DuckDB errors.
//!
//! duckdb-rs reports every failed DuckDB call as
//! [`duckdb::Error::DuckDBFailure`] with an unknown error code: it never reads
//! the exception type DuckDB records for the failure. DuckDB writes that type
//! at the start of every error message, as `"<type> Error: <message>"`, so
//! DuckLake recovers it from there.

use etl::error::Retryability;

use crate::retry::ClassifiedError;

/// Separator DuckDB writes between the exception type and the message.
const TYPE_SEPARATOR: &str = " Error: ";
/// Prefix DuckDB writes for [`DuckDbErrorType::UnknownType`], which has no
/// type name.
const UNNAMED_TYPE_PREFIX: &str = "Error: ";

/// The DuckDB exception type that caused an error.
///
/// Mirrors DuckDB's `ExceptionType`. Each variant parses from the name in
/// DuckDB's `EXCEPTION_MAP` (`src/common/exception.cpp`), so keep both in step
/// when upgrading DuckDB.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DuckDbErrorType {
    /// Messages starting with `Invalid Error:`.
    Invalid,
    /// Messages starting with `Out of Range Error:`.
    OutOfRange,
    /// Messages starting with `Conversion Error:`.
    Conversion,
    /// Messages starting with `Error:`. DuckDB omits the type name for this
    /// type.
    UnknownType,
    /// Messages starting with `Decimal Error:`.
    Decimal,
    /// Messages starting with `Mismatch Type Error:`.
    MismatchType,
    /// Messages starting with `Divide by Zero Error:`.
    DivideByZero,
    /// Messages starting with `Object Size Error:`.
    ObjectSize,
    /// Messages starting with `Invalid type Error:`.
    InvalidType,
    /// Messages starting with `Serialization Error:`.
    Serialization,
    /// Messages starting with `TransactionContext Error:`.
    Transaction,
    /// Messages starting with `Not implemented Error:`.
    NotImplemented,
    /// Messages starting with `Expression Error:`.
    Expression,
    /// Messages starting with `Catalog Error:`.
    Catalog,
    /// Messages starting with `Parser Error:`.
    Parser,
    /// Messages starting with `Binder Error:`.
    Binder,
    /// Messages starting with `Planner Error:`.
    Planner,
    /// Messages starting with `Scheduler Error:`.
    Scheduler,
    /// Messages starting with `Executor Error:`.
    Executor,
    /// Messages starting with `Constraint Error:`.
    Constraint,
    /// Messages starting with `Index Error:`.
    Index,
    /// Messages starting with `Stat Error:`.
    Stat,
    /// Messages starting with `Connection Error:`.
    Connection,
    /// Messages starting with `Syntax Error:`.
    Syntax,
    /// Messages starting with `Settings Error:`.
    Settings,
    /// Messages starting with `Optimizer Error:`.
    Optimizer,
    /// Messages starting with `NullPointer Error:`.
    NullPointer,
    /// Messages starting with `IO Error:`.
    Io,
    /// Messages starting with `INTERRUPT Error:`.
    Interrupt,
    /// Messages starting with `FATAL Error:`.
    Fatal,
    /// Messages starting with `INTERNAL Error:`.
    Internal,
    /// Messages starting with `Invalid Input Error:`.
    InvalidInput,
    /// Messages starting with `Out of Memory Error:`.
    OutOfMemory,
    /// Messages starting with `Permission Error:`.
    Permission,
    /// Messages starting with `Parameter Not Resolved Error:`.
    ParameterNotResolved,
    /// Messages starting with `Parameter Not Allowed Error:`.
    ParameterNotAllowed,
    /// Messages starting with `Dependency Error:`.
    Dependency,
    /// Messages starting with `Missing Extension Error:`.
    MissingExtension,
    /// Messages starting with `HTTP Error:`.
    Http,
    /// Messages starting with `Extension Autoloading Error:`.
    Autoload,
    /// Messages starting with `Sequence Error:`.
    Sequence,
    /// Messages starting with `Invalid Configuration Error:`.
    InvalidConfiguration,
}

impl DuckDbErrorType {
    /// Parses the exception type from a DuckDB error message.
    ///
    /// Returns [`None`] when the message does not start with a known DuckDB
    /// exception type, such as messages duckdb-rs writes itself.
    pub(super) fn parse(message: &str) -> Option<Self> {
        if message.starts_with(UNNAMED_TYPE_PREFIX) {
            return Some(Self::UnknownType);
        }

        let (name, _) = message.split_once(TYPE_SEPARATOR)?;
        let error_type = match name {
            "Invalid" => Self::Invalid,
            "Out of Range" => Self::OutOfRange,
            "Conversion" => Self::Conversion,
            "Unknown Type" => Self::UnknownType,
            "Decimal" => Self::Decimal,
            "Mismatch Type" => Self::MismatchType,
            "Divide by Zero" => Self::DivideByZero,
            "Object Size" => Self::ObjectSize,
            "Invalid type" => Self::InvalidType,
            "Serialization" => Self::Serialization,
            "TransactionContext" => Self::Transaction,
            "Not implemented" => Self::NotImplemented,
            "Expression" => Self::Expression,
            "Catalog" => Self::Catalog,
            "Parser" => Self::Parser,
            "Binder" => Self::Binder,
            "Planner" => Self::Planner,
            "Scheduler" => Self::Scheduler,
            "Executor" => Self::Executor,
            "Constraint" => Self::Constraint,
            "Index" => Self::Index,
            "Stat" => Self::Stat,
            "Connection" => Self::Connection,
            "Syntax" => Self::Syntax,
            "Settings" => Self::Settings,
            "Optimizer" => Self::Optimizer,
            "NullPointer" => Self::NullPointer,
            "IO" => Self::Io,
            "INTERRUPT" => Self::Interrupt,
            "FATAL" => Self::Fatal,
            "INTERNAL" => Self::Internal,
            "Invalid Input" => Self::InvalidInput,
            "Out of Memory" => Self::OutOfMemory,
            "Permission" => Self::Permission,
            "Parameter Not Resolved" => Self::ParameterNotResolved,
            "Parameter Not Allowed" => Self::ParameterNotAllowed,
            "Dependency" => Self::Dependency,
            "Missing Extension" => Self::MissingExtension,
            "HTTP" => Self::Http,
            "Extension Autoloading" => Self::Autoload,
            "Sequence" => Self::Sequence,
            "Invalid Configuration" => Self::InvalidConfiguration,
            _ => return None,
        };

        Some(error_type)
    }

    /// Returns whether a statement that failed with this type may succeed when
    /// run again.
    pub(super) fn retryability(self) -> Retryability {
        match self {
            // A concurrent writer won a DuckLake catalog commit, or the catalog,
            // object storage, or network failed. DuckDB's Postgres extension
            // reports lost catalog connections and failed catalog queries as
            // I/O errors. An interrupt comes from DuckLake's own query deadline
            // or shutdown, not from the statement. Memory pressure from
            // concurrent work may ease.
            Self::Transaction
            | Self::Io
            | Self::Http
            | Self::Connection
            | Self::Interrupt
            | Self::OutOfMemory => Retryability::Retryable,

            // The statement, its data, or the lake's state is rejected the
            // same way until something changes. `Invalid` covers non-DuckDB
            // exceptions, `Fatal` an invalidated database, and `Internal` a
            // failed DuckDB assertion.
            Self::Invalid
            | Self::OutOfRange
            | Self::Conversion
            | Self::UnknownType
            | Self::Decimal
            | Self::MismatchType
            | Self::DivideByZero
            | Self::ObjectSize
            | Self::InvalidType
            | Self::Serialization
            | Self::NotImplemented
            | Self::Expression
            | Self::Catalog
            | Self::Parser
            | Self::Binder
            | Self::Planner
            | Self::Scheduler
            | Self::Executor
            | Self::Constraint
            | Self::Index
            | Self::Stat
            | Self::Syntax
            | Self::Settings
            | Self::Optimizer
            | Self::NullPointer
            | Self::Fatal
            | Self::Internal
            | Self::InvalidInput
            | Self::Permission
            | Self::ParameterNotResolved
            | Self::ParameterNotAllowed
            | Self::Dependency
            | Self::MissingExtension
            | Self::Autoload
            | Self::Sequence
            | Self::InvalidConfiguration => Retryability::Permanent,
        }
    }
}

/// Returns the exception type of a failed DuckDB call, when DuckDB reported
/// one.
pub(super) fn duckdb_error_type(error: &duckdb::Error) -> Option<DuckDbErrorType> {
    match error {
        duckdb::Error::DuckDBFailure(_, Some(message)) => DuckDbErrorType::parse(message),
        _ => None,
    }
}

/// A DuckDB failure follows its exception type. A failure without a known
/// type, and every error duckdb-rs raises itself, such as a column conversion
/// or a misused statement, fails the same way when run again.
impl ClassifiedError for duckdb::Error {
    fn retryability(&self) -> Retryability {
        duckdb_error_type(self).map_or(Retryability::Permanent, DuckDbErrorType::retryability)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a failed DuckDB call that reported `message`.
    fn duckdb_failure(message: &str) -> duckdb::Error {
        duckdb::Error::DuckDBFailure(duckdb::ffi::Error::new(1), Some(message.to_owned()))
    }

    #[test]
    fn parse_reads_the_exception_type_prefix() {
        for (message, expected) in [
            ("Catalog Error: Table with name t does not exist!", DuckDbErrorType::Catalog),
            (
                "TransactionContext Error: Failed to commit: Failed to commit DuckLake \
                 transaction: conflict",
                DuckDbErrorType::Transaction,
            ),
            ("IO Error: Unable to connect to Postgres at host", DuckDbErrorType::Io),
            ("INTERRUPT Error: Interrupted!", DuckDbErrorType::Interrupt),
            ("Invalid Input Error: bad value", DuckDbErrorType::InvalidInput),
            ("Invalid Error: std::exception", DuckDbErrorType::Invalid),
            ("Invalid type Error: type", DuckDbErrorType::InvalidType),
            ("Error: Type with name foo does not exist", DuckDbErrorType::UnknownType),
            ("HTTP Error: HTTP GET error", DuckDbErrorType::Http),
        ] {
            assert_eq!(DuckDbErrorType::parse(message), Some(expected), "{message}");
        }
    }

    #[test]
    fn parse_rejects_messages_without_a_known_type() {
        for message in [
            "",
            "connect error",
            "Error",
            " Error: leading separator",
            "Unknown Error: not a DuckDB type name",
            "catalog Error: names are case-sensitive",
            "query failed: Catalog Error: type not at the start",
        ] {
            assert_eq!(DuckDbErrorType::parse(message), None, "{message:?}");
        }
    }

    #[test]
    fn duckdb_failures_follow_their_exception_type() {
        for (message, expected) in [
            ("TransactionContext Error: Failed to commit", Retryability::Retryable),
            ("IO Error: Could not read file", Retryability::Retryable),
            ("HTTP Error: HTTP GET error", Retryability::Retryable),
            ("Connection Error: connection closed", Retryability::Retryable),
            ("INTERRUPT Error: Interrupted!", Retryability::Retryable),
            ("Out of Memory Error: failed to allocate", Retryability::Retryable),
            ("Constraint Error: NOT NULL constraint failed", Retryability::Permanent),
            ("Catalog Error: Table with name t does not exist!", Retryability::Permanent),
            ("Parser Error: syntax error at or near", Retryability::Permanent),
            ("Binder Error: Referenced column not found", Retryability::Permanent),
            ("Conversion Error: Could not convert", Retryability::Permanent),
            ("FATAL Error: database has been invalidated", Retryability::Permanent),
            ("INTERNAL Error: assertion failed", Retryability::Permanent),
            ("connect error", Retryability::Permanent),
        ] {
            assert_eq!(duckdb_failure(message).retryability(), expected, "{message}");
        }
    }

    #[test]
    fn errors_raised_by_duckdb_rs_are_permanent() {
        for error in [
            duckdb::Error::DuckDBFailure(duckdb::ffi::Error::new(1), None),
            duckdb::Error::QueryReturnedNoRows,
            duckdb::Error::InvalidColumnIndex(3),
        ] {
            assert_eq!(error.retryability(), Retryability::Permanent);
        }
    }

    /// Pins the message format of the bundled DuckDB, which this module
    /// parses because duckdb-rs drops the exception type.
    #[test]
    fn bundled_duckdb_failures_parse_to_their_exception_type() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("create table t (id integer not null);").unwrap();

        for (sql, expected) in [
            ("select * from missing_table", DuckDbErrorType::Catalog),
            ("selec 1", DuckDbErrorType::Parser),
            ("select missing_column from t", DuckDbErrorType::Binder),
            ("insert into t values (null)", DuckDbErrorType::Constraint),
            ("select 'x'::integer", DuckDbErrorType::Conversion),
            ("select * from read_csv('/nonexistent/etl-duckdb-error.csv')", DuckDbErrorType::Io),
            // Leaves the connection inside a transaction, so it runs last.
            ("begin; begin;", DuckDbErrorType::Transaction),
        ] {
            let error = conn.execute_batch(sql).unwrap_err();
            assert_eq!(duckdb_error_type(&error), Some(expected), "{sql}: {error}");
        }
    }
}
