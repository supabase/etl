//! Macros for ETL error handling.
//!
//! Provides convenience macros for creating and returning [`crate::error::EtlError`] instances with
//! reduced boilerplate for common error handling patterns.

/// Creates an [`crate::error::EtlError`] from error kind and description.
///
/// This macro provides a concise way to create [`crate::error::EtlError`] instances with
/// static descriptions, optional dynamic detail (use `detail =` to move an owned [`String`]),
/// and optional source errors.
#[macro_export]
macro_rules! etl_error {
    ($kind:expr, $desc:expr) => {
        $crate::error::EtlError::from(($kind, $desc))
    };
    ($kind:expr, $desc:expr, source: $source:expr) => {
        $crate::error::EtlError::from(($kind, $desc)).with_source($source)
    };
    ($kind:expr, $desc:expr, detail = $detail:expr) => {
        $crate::error::EtlError::from(($kind, $desc, $detail))
    };
    ($kind:expr, $desc:expr, detail = $detail:expr, source: $source:expr) => {
        $crate::error::EtlError::from(($kind, $desc, $detail)).with_source($source)
    };
    ($kind:expr, $desc:expr, $detail:expr) => {
        $crate::error::EtlError::from(($kind, $desc, $detail.to_string()))
    };
    ($kind:expr, $desc:expr, $detail:expr, source: $source:expr) => {
        $crate::error::EtlError::from(($kind, $desc, $detail.to_string())).with_source($source)
    };
}

/// Creates and returns an [`crate::error::EtlError`] from the current function.
///
/// This macro combines error creation with early return, reducing boilerplate
/// when handling error conditions that should immediately terminate execution.
/// Supports the same optional detail and source arguments as [`etl_error!`].
#[macro_export]
macro_rules! bail {
    ($kind:expr, $desc:expr) => {
        return ::core::result::Result::Err($crate::etl_error!($kind, $desc))
    };
    ($kind:expr, $desc:expr, source: $source:expr) => {
        return ::core::result::Result::Err($crate::etl_error!($kind, $desc, source: $source))
    };
    ($kind:expr, $desc:expr, detail = $detail:expr) => {
        return ::core::result::Result::Err($crate::etl_error!($kind, $desc, detail = $detail))
    };
    ($kind:expr, $desc:expr, detail = $detail:expr, source: $source:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(
            $kind,
            $desc,
            detail = $detail,
            source: $source
        ))
    };
    ($kind:expr, $desc:expr, $detail:expr) => {
        return ::core::result::Result::Err($crate::etl_error!($kind, $desc, $detail))
    };
    ($kind:expr, $desc:expr, $detail:expr, source: $source:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(
            $kind,
            $desc,
            $detail,
            source: $source
        ))
    };
}
