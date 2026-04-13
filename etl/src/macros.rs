//! Macros for ETL error handling.
//!
//! Provides convenience macros for creating and returning [`crate::error::EtlError`] instances with
//! reduced boilerplate for common error handling patterns.

/// Maps macro scope identifiers to [`crate::error::ErrorScope`].
#[doc(hidden)]
#[macro_export]
macro_rules! __etl_error_scope {
    (source) => {
        $crate::error::ErrorScope::Source
    };
    (destination) => {
        $crate::error::ErrorScope::Destination
    };
    (internal) => {
        $crate::error::ErrorScope::Internal
    };
}

/// Creates an [`crate::error::EtlError`] from error scope, class, and description.
#[macro_export]
macro_rules! etl_error {
    (source, $class:expr, $desc:expr) => {
        $crate::error::EtlError::from_scope_class_parts(
            $crate::__etl_error_scope!(source),
            $class,
            $desc,
            None,
        )
    };
    (source, $class:expr, $desc:expr, source: $source:expr) => {
        $crate::error::EtlError::from_scope_class_parts(
            $crate::__etl_error_scope!(source),
            $class,
            $desc,
            None,
        )
        .with_source($source)
    };
    (source, $class:expr, $desc:expr, $detail:expr) => {
        $crate::error::EtlError::from_scope_class_parts(
            $crate::__etl_error_scope!(source),
            $class,
            $desc,
            Some(($detail).to_string().into()),
        )
    };
    (source, $class:expr, $desc:expr, $detail:expr, source: $source:expr) => {
        $crate::error::EtlError::from_scope_class_parts(
            $crate::__etl_error_scope!(source),
            $class,
            $desc,
            Some(($detail).to_string().into()),
        )
        .with_source($source)
    };
    (destination, $class:expr, $desc:expr) => {
        $crate::error::EtlError::from_scope_class_parts(
            $crate::__etl_error_scope!(destination),
            $class,
            $desc,
            None,
        )
    };
    (destination, $class:expr, $desc:expr, source: $source:expr) => {
        $crate::error::EtlError::from_scope_class_parts(
            $crate::__etl_error_scope!(destination),
            $class,
            $desc,
            None,
        )
        .with_source($source)
    };
    (destination, $class:expr, $desc:expr, $detail:expr) => {
        $crate::error::EtlError::from_scope_class_parts(
            $crate::__etl_error_scope!(destination),
            $class,
            $desc,
            Some(($detail).to_string().into()),
        )
    };
    (destination, $class:expr, $desc:expr, $detail:expr, source: $source:expr) => {
        $crate::error::EtlError::from_scope_class_parts(
            $crate::__etl_error_scope!(destination),
            $class,
            $desc,
            Some(($detail).to_string().into()),
        )
        .with_source($source)
    };
    (internal, $class:expr, $desc:expr) => {
        $crate::error::EtlError::from_scope_class_parts(
            $crate::__etl_error_scope!(internal),
            $class,
            $desc,
            None,
        )
    };
    (internal, $class:expr, $desc:expr, source: $source:expr) => {
        $crate::error::EtlError::from_scope_class_parts(
            $crate::__etl_error_scope!(internal),
            $class,
            $desc,
            None,
        )
        .with_source($source)
    };
    (internal, $class:expr, $desc:expr, $detail:expr) => {
        $crate::error::EtlError::from_scope_class_parts(
            $crate::__etl_error_scope!(internal),
            $class,
            $desc,
            Some(($detail).to_string().into()),
        )
    };
    (internal, $class:expr, $desc:expr, $detail:expr, source: $source:expr) => {
        $crate::error::EtlError::from_scope_class_parts(
            $crate::__etl_error_scope!(internal),
            $class,
            $desc,
            Some(($detail).to_string().into()),
        )
        .with_source($source)
    };
    (scope: $scope:expr, $class:expr, $desc:expr) => {
        $crate::error::EtlError::from_scope_class_parts($scope, $class, $desc, None)
    };
    (scope: $scope:expr, $class:expr, $desc:expr, source: $source:expr) => {
        $crate::error::EtlError::from_scope_class_parts($scope, $class, $desc, None)
            .with_source($source)
    };
    (scope: $scope:expr, $class:expr, $desc:expr, $detail:expr) => {
        $crate::error::EtlError::from_scope_class_parts(
            $scope,
            $class,
            $desc,
            Some(($detail).to_string().into()),
        )
    };
    (scope: $scope:expr, $class:expr, $desc:expr, $detail:expr, source: $source:expr) => {
        $crate::error::EtlError::from_scope_class_parts(
            $scope,
            $class,
            $desc,
            Some(($detail).to_string().into()),
        )
        .with_source($source)
    };
}

/// Creates and returns an [`crate::error::EtlError`] from the current function.
///
/// This macro combines error creation with early return, reducing boilerplate
/// when handling error conditions that should immediately terminate execution.
/// Supports the same optional detail and source arguments as [`etl_error!`].
#[macro_export]
macro_rules! bail {
    (source, $class:expr, $desc:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(source, $class, $desc))
    };
    (source, $class:expr, $desc:expr, source: $source:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(
            source,
            $class,
            $desc,
            source: $source
        ))
    };
    (source, $class:expr, $desc:expr, $detail:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(source, $class, $desc, $detail))
    };
    (source, $class:expr, $desc:expr, $detail:expr, source: $source:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(
            source,
            $class,
            $desc,
            $detail,
            source: $source
        ))
    };
    (destination, $class:expr, $desc:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(destination, $class, $desc))
    };
    (destination, $class:expr, $desc:expr, source: $source:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(
            destination,
            $class,
            $desc,
            source: $source
        ))
    };
    (destination, $class:expr, $desc:expr, $detail:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(
            destination,
            $class,
            $desc,
            $detail
        ))
    };
    (destination, $class:expr, $desc:expr, $detail:expr, source: $source:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(
            destination,
            $class,
            $desc,
            $detail,
            source: $source
        ))
    };
    (internal, $class:expr, $desc:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(internal, $class, $desc))
    };
    (internal, $class:expr, $desc:expr, source: $source:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(
            internal,
            $class,
            $desc,
            source: $source
        ))
    };
    (internal, $class:expr, $desc:expr, $detail:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(
            internal,
            $class,
            $desc,
            $detail
        ))
    };
    (internal, $class:expr, $desc:expr, $detail:expr, source: $source:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(
            internal,
            $class,
            $desc,
            $detail,
            source: $source
        ))
    };
    (scope: $scope:expr, $class:expr, $desc:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(scope: $scope, $class, $desc))
    };
    (scope: $scope:expr, $class:expr, $desc:expr, source: $source:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(
            scope: $scope,
            $class,
            $desc,
            source: $source
        ))
    };
    (scope: $scope:expr, $class:expr, $desc:expr, $detail:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(scope: $scope, $class, $desc, $detail))
    };
    (scope: $scope:expr, $class:expr, $desc:expr, $detail:expr, source: $source:expr) => {
        return ::core::result::Result::Err($crate::etl_error!(
            scope: $scope,
            $class,
            $desc,
            $detail,
            source: $source
        ))
    };
}
