use crate::bail;
use crate::error::EtlError;
use crate::error::{ErrorKind, EtlResult};

/// Converts a Postgres bytea hex string to a byte array.
///
/// This function parses Postgres's hex-encoded bytea format, which uses
/// the `\x` prefix followed by hexadecimal digits. Each pair of hex digits
/// represents one byte in the output array.
pub fn parse_bytea_hex_string(value: &str) -> EtlResult<Vec<u8>> {
    if value.len() < 2 || &value[..2] != "\\x" {
        bail!(
            ErrorKind::ConversionError,
            "Could not convert from bytea hex string to byte array",
            "The prefix '\\x' is missing"
        );
    }

    let mut result = Vec::with_capacity((value.len() - 2) / 2);

    let value = &value[2..];
    if value.len() % 2 != 0 {
        bail!(
            ErrorKind::ConversionError,
            "Could not convert from bytea hex string to byte array",
            "The number of digits is odd"
        );
    }

    for i in (0..value.len()).step_by(2) {
        let val = u8::from_str_radix(&value[i..i + 2], 16)?;
        result.push(val);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    #[test]
    fn parse_bytea_hex_empty() {
        let result = parse_bytea_hex_string("\\x").unwrap();
        assert_eq!(result, Vec::<u8>::new());
    }

    #[test]
    fn parse_bytea_hex_single_byte() {
        let result = parse_bytea_hex_string("\\x41").unwrap();
        assert_eq!(result, vec![0x41]);
    }

    #[test]
    fn parse_bytea_hex_multiple_bytes() {
        let result = parse_bytea_hex_string("\\x48656c6c6f").unwrap();
        assert_eq!(result, b"Hello");
    }

    #[test]
    fn parse_bytea_hex_all_zero() {
        let result = parse_bytea_hex_string("\\x0000").unwrap();
        assert_eq!(result, vec![0x00, 0x00]);
    }

    #[test]
    fn parse_bytea_hex_all_ff() {
        let result = parse_bytea_hex_string("\\xffff").unwrap();
        assert_eq!(result, vec![0xff, 0xff]);
    }

    #[test]
    fn parse_bytea_hex_mixed_case() {
        let result = parse_bytea_hex_string("\\xaBcD").unwrap();
        assert_eq!(result, vec![0xab, 0xcd]);
    }

    #[test]
    fn parse_bytea_hex_long_sequence() {
        let result = parse_bytea_hex_string("\\x0123456789abcdef").unwrap();
        assert_eq!(result, vec![0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]);
    }

    #[test]
    fn parse_bytea_hex_missing_prefix() {
        let result = parse_bytea_hex_string("41");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ConversionError));
        assert!(err.to_string().contains("prefix '\\x' is missing"));
    }

    #[test]
    fn parse_bytea_hex_wrong_prefix() {
        let result = parse_bytea_hex_string("0x41");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ConversionError));
        assert!(err.to_string().contains("prefix '\\x' is missing"));
    }

    #[test]
    fn parse_bytea_hex_empty_string() {
        let result = parse_bytea_hex_string("");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ConversionError));
        assert!(err.to_string().contains("prefix '\\x' is missing"));
    }

    #[test]
    fn parse_bytea_hex_only_prefix() {
        let result = parse_bytea_hex_string("\\");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ConversionError));
        assert!(err.to_string().contains("prefix '\\x' is missing"));
    }

    #[test]
    fn parse_bytea_hex_odd_length() {
        let result = parse_bytea_hex_string("\\x4");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ConversionError));
        assert!(err.to_string().contains("number of digits is odd"));
    }

    #[test]
    fn parse_bytea_hex_odd_length_multiple() {
        let result = parse_bytea_hex_string("\\x41424");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ConversionError));
        assert!(err.to_string().contains("number of digits is odd"));
    }

    #[test]
    fn parse_bytea_hex_invalid_hex_char() {
        let result = parse_bytea_hex_string("\\x4g");
        assert!(result.is_err());
        // This should be a parsing error from from_str_radix
        let err = result.unwrap_err();
        // The error should propagate from the underlying parsing
        assert!(err.to_string().contains("invalid digit"));
    }

    #[test]
    fn parse_bytea_hex_invalid_hex_chars() {
        assert!(parse_bytea_hex_string("\\xgg").is_err());
        assert!(parse_bytea_hex_string("\\x4z").is_err());
        assert!(parse_bytea_hex_string("\\xZZ").is_err());
    }

    #[test]
    fn parse_bytea_hex_non_ascii() {
        let result = parse_bytea_hex_string("\\x4🤔");
        assert!(result.is_err());
    }

    #[test]
    fn parse_bytea_hex_whitespace() {
        let result = parse_bytea_hex_string("\\x4 1");
        assert!(result.is_err());
    }

    #[test]
    fn parse_bytea_hex_with_separator() {
        let result = parse_bytea_hex_string("\\x41-42");
        assert!(result.is_err());
    }

    #[test]
    fn parse_bytea_hex_binary_data() {
        // Test conversion of various binary data patterns
        let result = parse_bytea_hex_string("\\x00010203040506070809").unwrap();
        assert_eq!(result, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn parse_bytea_hex_capacity_optimization() {
        // Test that the Vec capacity is set correctly
        let result = parse_bytea_hex_string("\\x414243444546").unwrap();
        assert_eq!(result, b"ABCDEF");
        // Vector should be exactly the right size
        assert_eq!(result.len(), 6);
    }
}
