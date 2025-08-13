//! Boolean value parsing for PostgreSQL COPY format data.
//!
//! This module provides parsing functionality for PostgreSQL boolean values as they appear
//! in COPY format output. PostgreSQL has specific text representations for boolean values
//! that differ from standard programming language conventions.

use crate::bail;
use crate::error::EtlResult;
use crate::error::{ErrorKind, EtlError};

/// Parses a PostgreSQL boolean value from its text format representation.
///
/// PostgreSQL uses a specific text format for boolean values in COPY output that differs
/// from common programming language conventions. This function implements strict parsing
/// according to PostgreSQL's exact format requirements.
///
/// # PostgreSQL Boolean Format
///
/// PostgreSQL represents boolean values in text format as single characters:
/// - `"t"` → `true` (exactly one lowercase 't')
/// - `"f"` → `false` (exactly one lowercase 'f')
///
/// # Strict Parsing Rules
///
/// This function enforces strict compliance with PostgreSQL's format:
/// - **Case sensitive**: Only lowercase 't' and 'f' are accepted
/// - **Exact length**: Must be exactly one character
/// - **No whitespace**: Leading/trailing whitespace is not trimmed
/// - **No alternatives**: Common formats like "true", "false", "1", "0" are rejected
///
/// # Design Rationale
///
/// ## Why Strict Parsing?
/// - **Data integrity**: Ensures consistent interpretation of PostgreSQL data
/// - **Error detection**: Catches potential data corruption or format inconsistencies
/// - **Predictability**: Eliminates ambiguity in boolean value interpretation
/// - **Compatibility**: Maintains exact compatibility with PostgreSQL's output format
///
/// ## Performance Considerations
/// - **Minimal overhead**: Simple string comparison without complex parsing logic
/// - **No allocations**: Operates directly on string slice without copying
/// - **Fail-fast**: Immediate error return for invalid values
/// - **Branch optimization**: Simple if-else structure optimizes well
///
/// # Error Handling
///
/// The function returns [`ErrorKind::InvalidData`] for any input that doesn't exactly
/// match PostgreSQL's boolean format. The error includes the received value for debugging.
///
/// ## Common Error Cases
/// - **Standard boolean strings**: "true", "false" (too verbose)
/// - **Numeric representations**: "1", "0" (not PostgreSQL format)
/// - **Case variations**: "T", "F", "True", "False" (wrong case)
/// - **Whitespace**: " t", "f ", " f " (contains whitespace)
/// - **Multiple characters**: "tt", "tf", "yes", "no" (wrong length)
/// - **Special characters**: "t\n", "f\t" (contains control characters)
/// - **Unicode variations**: Various Unicode boolean-like characters
///
/// # Examples
///
/// ```rust,no_run
/// # fn parse_bool(s: &str) -> Result<bool, Box<dyn std::error::Error>> {
/// #     match s { "t" => Ok(true), "f" => Ok(false), _ => Err("invalid".into()) }
/// # }
///
/// // Valid PostgreSQL boolean values
/// assert_eq!(parse_bool("t").unwrap(), true);
/// assert_eq!(parse_bool("f").unwrap(), false);
///
/// // Invalid values that will return errors
/// assert!(parse_bool("true").is_err());   // Too verbose
/// assert!(parse_bool("1").is_err());      // Numeric format
/// assert!(parse_bool("T").is_err());      // Wrong case
/// assert!(parse_bool(" t").is_err());     // Contains whitespace
/// assert!(parse_bool("").is_err());       // Empty string
/// ```
///
/// # Integration Notes
///
/// This function is typically called as part of the table row conversion process
/// when processing COPY format data from PostgreSQL. It should not be used for
/// parsing boolean values from other sources that may use different formats.
pub fn parse_bool(s: &str) -> EtlResult<bool> {
    if s == "t" {
        Ok(true)
    } else if s == "f" {
        Ok(false)
    } else {
        bail!(
            ErrorKind::InvalidData,
            "Invalid boolean value",
            format!("Boolean value must be 't' or 'f' (received: {s})")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    #[test]
    fn parse_bool_true() {
        assert!(parse_bool("t").unwrap());
    }

    #[test]
    fn parse_bool_false() {
        assert!(!parse_bool("f").unwrap());
    }

    #[test]
    fn parse_bool_invalid_empty() {
        let result = parse_bool("");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidData));
        assert!(err.to_string().contains("Boolean value must be 't' or 'f'"));
        assert!(err.to_string().contains("received: "));
    }

    #[test]
    fn parse_bool_invalid_true_word() {
        let result = parse_bool("true");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidData));
        assert!(err.to_string().contains("received: true"));
    }

    #[test]
    fn parse_bool_invalid_false_word() {
        let result = parse_bool("false");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidData));
        assert!(err.to_string().contains("received: false"));
    }

    #[test]
    fn parse_bool_invalid_numbers() {
        assert!(parse_bool("0").is_err());
        assert!(parse_bool("1").is_err());
    }

    #[test]
    fn parse_bool_invalid_case_sensitive() {
        assert!(parse_bool("T").is_err());
        assert!(parse_bool("F").is_err());
    }

    #[test]
    fn parse_bool_invalid_whitespace() {
        assert!(parse_bool(" t").is_err());
        assert!(parse_bool("t ").is_err());
        assert!(parse_bool(" f ").is_err());
    }

    #[test]
    fn parse_bool_invalid_special_chars() {
        assert!(parse_bool("t\n").is_err());
        assert!(parse_bool("f\t").is_err());
        assert!(parse_bool("t\0").is_err());
    }

    #[test]
    fn parse_bool_invalid_unicode() {
        assert!(parse_bool("🤔").is_err());
        assert!(parse_bool("ÿ").is_err());
    }

    #[test]
    fn parse_bool_invalid_multiple_chars() {
        assert!(parse_bool("tt").is_err());
        assert!(parse_bool("tf").is_err());
        assert!(parse_bool("ft").is_err());
        assert!(parse_bool("ff").is_err());
    }
}
