//! MySQL version constants and utilities.
//!
//! This module provides version constants for supported MySQL versions and macros
//! for ergonomic version comparison. Version numbers follow MySQL's internal format:
//! `MAJOR * 10000 + MINOR * 100 + PATCH`.
//!
//! # Supported Versions
//!
//! ETL officially supports MySQL versions 5.7, 8.0, 8.1, 8.2, 8.3, and 8.4.

use std::num::NonZeroI32;

pub const MYSQL_5_7: i32 = 50700;
pub const MYSQL_8_0: i32 = 80000;
pub const MYSQL_8_1: i32 = 80100;
pub const MYSQL_8_2: i32 = 80200;
pub const MYSQL_8_3: i32 = 80300;
pub const MYSQL_8_4: i32 = 80400;

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
/// in older MySQL versions.
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
        assert_eq!(MYSQL_5_7, 50700);
        assert_eq!(MYSQL_8_0, 80000);
        assert_eq!(MYSQL_8_1, 80100);
        assert_eq!(MYSQL_8_2, 80200);
        assert_eq!(MYSQL_8_3, 80300);
        assert_eq!(MYSQL_8_4, 80400);
    }

    #[test]
    fn test_meets_version_with_some() {
        let version = NonZeroI32::new(80035);
        assert!(meets_version(version, MYSQL_5_7));
        assert!(meets_version(version, MYSQL_8_0));
        assert!(!meets_version(version, MYSQL_8_1));
        assert!(!meets_version(version, MYSQL_8_2));
    }

    #[test]
    fn test_meets_version_with_none() {
        assert!(!meets_version(None, MYSQL_5_7));
        assert!(!meets_version(None, MYSQL_8_0));
        assert!(!meets_version(None, MYSQL_8_1));
    }

    #[test]
    fn test_meets_version_exact_match() {
        let version = NonZeroI32::new(MYSQL_8_0);
        assert!(meets_version(version, MYSQL_8_0));
    }

    #[test]
    fn test_requires_version_macro() {
        let version = NonZeroI32::new(80200);
        assert!(requires_version!(version, MYSQL_5_7));
        assert!(requires_version!(version, MYSQL_8_0));
        assert!(requires_version!(version, MYSQL_8_1));
        assert!(requires_version!(version, MYSQL_8_2));
        assert!(!requires_version!(version, MYSQL_8_3));
    }

    #[test]
    fn test_below_version_macro() {
        let version = NonZeroI32::new(50730);
        assert!(!below_version!(version, MYSQL_5_7));
        assert!(below_version!(version, MYSQL_8_0));
        assert!(below_version!(version, MYSQL_8_1));
    }

    #[test]
    fn test_requires_version_with_none() {
        let version: Option<NonZeroI32> = None;
        assert!(!requires_version!(version, MYSQL_8_0));
    }
}
