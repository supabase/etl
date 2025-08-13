//! Macros for ETL error handling.
//!
//! Provides convenience macros for creating and returning [`crate::error::EtlError`] instances with
//! reduced boilerplate for common error handling patterns.

/// Creates an [`crate::error::EtlError`] from error kind and description.
///
/// This macro provides a concise way to create [`crate::error::EtlError`] instances with
/// either static descriptions or additional dynamic detail information.
#[macro_export]
macro_rules! etl_error {
    ($kind:expr, $desc:expr) => {
        EtlError::from(($kind, $desc))
    };
    ($kind:expr, $desc:expr, $detail:expr) => {
        EtlError::from(($kind, $desc, $detail.to_string()))
    };
}

/// Creates and returns an [`crate::error::EtlError`] from the current function.
///
/// This macro combines error creation with early return, reducing boilerplate
/// when handling error conditions that should immediately terminate execution.
#[macro_export]
macro_rules! bail {
    ($kind:expr, $desc:expr) => {
        return Err($crate::etl_error!($kind, $desc))
    };
    ($kind:expr, $desc:expr, $detail:expr) => {
        return Err($crate::etl_error!($kind, $desc, $detail))
    };
}
