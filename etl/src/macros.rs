//! Utility macros for error handling and common operations.
//!
//! This module provides convenient macros for creating and returning ETL errors
//! with consistent formatting and reduced boilerplate code.

/// Creates an [`crate::error::EtlError`] from error kind and description.
///
/// This macro provides a concise way to create [`crate::error::EtlError`] instances with
/// either static descriptions or additional dynamic detail information.
///
/// # Examples
/// ```rust,no_run
/// use etl::prelude::*;
///
/// // Simple error with static description
/// let error = etl_error!(ErrorKind::ValidationError, "Invalid input");
///
/// // Error with additional detail  
/// let error = etl_error!(
///     ErrorKind::ConversionError,
///     "Type conversion failed",
///     format!("Cannot convert {} to integer", "abc")
/// );
/// ```
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
///
/// # Examples
/// ```rust,no_run
/// use etl::prelude::*;
///
/// fn validate_input(value: i32) -> EtlResult<i32> {
///     if value < 0 {
///         bail!(ErrorKind::ValidationError, "Value must be positive");
///     }
///     Ok(value)
/// }
/// ```
#[macro_export]
macro_rules! bail {
    ($kind:expr, $desc:expr) => {
        return Err($crate::etl_error!($kind, $desc))
    };
    ($kind:expr, $desc:expr, $detail:expr) => {
        return Err($crate::etl_error!($kind, $desc, $detail))
    };
}
