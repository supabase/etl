//! PostgreSQL version constants and utilities.
//!
//! This module provides version constants for supported PostgreSQL versions and macros
//! for ergonomic version comparison. Version numbers follow PostgreSQL's internal format:
//! `MAJOR * 10000 + MINOR * 100 + PATCH`.
//!
//! # Supported Versions
//!
//! ETL officially supports PostgreSQL versions 14, 15, 16, and 17.

use std::num::NonZeroI32;

pub const POSTGRES_14: i32 = 140000;
pub const POSTGRES_15: i32 = 150000;
pub const POSTGRES_16: i32 = 160000;
pub const POSTGRES_17: i32 = 170000;

/// Returns [`true`] if the server version meets or exceeds the required version.
///
/// This function handles [`None`] server versions by returning [`false`], making it
/// safe to use in contexts where version information might not be available.
pub fn meets_version(server_version: Option<NonZeroI32>, required_version: i32) -> bool {
    server_version.is_some_and(|v| v.get() >= required_version)
}

/// Checks if the server version meets or exceeds the required version.
///
/// This macro provides ergonomic version checking by accepting various input types
/// for the server version (Option<NonZeroI32>, NonZeroI32, i32) and comparing against
/// version constants.
#[macro_export]
macro_rules! requires_version {
    ($server_version:expr, $required:expr) => {
        $crate::version::meets_version($server_version, $required)
    };
}

/// Checks if the server version is below the specified version.
///
/// This macro is useful for conditional logic when features are not available
/// in older PostgreSQL versions.
#[macro_export]
macro_rules! below_version {
    ($server_version:expr, $required:expr) => {
        !$crate::version::meets_version($server_version, $required)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_constants() {
        assert_eq!(POSTGRES_14, 140000);
        assert_eq!(POSTGRES_15, 150000);
        assert_eq!(POSTGRES_16, 160000);
        assert_eq!(POSTGRES_17, 170000);
    }

    #[test]
    fn test_meets_version_with_some() {
        let version = NonZeroI32::new(150500);
        assert!(meets_version(version, POSTGRES_14));
        assert!(meets_version(version, POSTGRES_15));
        assert!(!meets_version(version, POSTGRES_16));
        assert!(!meets_version(version, POSTGRES_17));
    }

    #[test]
    fn test_meets_version_with_none() {
        assert!(!meets_version(None, POSTGRES_14));
        assert!(!meets_version(None, POSTGRES_15));
        assert!(!meets_version(None, POSTGRES_16));
    }

    #[test]
    fn test_meets_version_exact_match() {
        let version = NonZeroI32::new(POSTGRES_15);
        assert!(meets_version(version, POSTGRES_15));
    }

    #[test]
    fn test_requires_version_macro() {
        let version = NonZeroI32::new(160200);
        assert!(requires_version!(version, POSTGRES_14));
        assert!(requires_version!(version, POSTGRES_15));
        assert!(requires_version!(version, POSTGRES_16));
        assert!(!requires_version!(version, POSTGRES_17));
    }

    #[test]
    fn test_below_version_macro() {
        let version = NonZeroI32::new(140800);
        assert!(!below_version!(version, POSTGRES_14));
        assert!(below_version!(version, POSTGRES_15));
        assert!(below_version!(version, POSTGRES_16));
        assert!(below_version!(version, POSTGRES_17));
    }

    #[test]
    fn test_requires_version_with_none() {
        let version: Option<NonZeroI32> = None;
        assert!(!requires_version!(version, POSTGRES_14));
    }
}
