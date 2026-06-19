use crate::{
    bail,
    error::{ErrorKind, EtlResult},
};

/// Converts a Postgres bytea hex string to a byte array.
///
/// This function parses Postgres's hex-encoded bytea format, which uses
/// the `\x` prefix followed by hexadecimal digits. Each pair of hex digits
/// represents one byte in the output array.
pub(crate) fn parse_bytea_hex_string(value: &str) -> EtlResult<Vec<u8>> {
    let value = value.as_bytes();
    if !value.starts_with(b"\\x") {
        bail!(
            ErrorKind::ConversionError,
            "Bytea hex string conversion failed",
            "Missing '\\x' prefix"
        );
    }

    let mut result = Vec::with_capacity((value.len() - 2) / 2);

    let value = &value[2..];
    if !value.len().is_multiple_of(2) {
        bail!(
            ErrorKind::ConversionError,
            "Bytea hex string conversion failed",
            "Odd number of hexadecimal digits"
        );
    }

    for digits in value.chunks_exact(2) {
        let high = parse_hex_digit(digits[0])?;
        let low = parse_hex_digit(digits[1])?;
        result.push((high << 4) | low);
    }

    Ok(result)
}

/// Parses one ASCII hexadecimal digit.
fn parse_hex_digit(byte: u8) -> EtlResult<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => {
            bail!(
                ErrorKind::ConversionError,
                "Bytea hex string conversion failed",
                "Invalid hexadecimal digit"
            );
        }
    }
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
        assert!(err.to_string().contains("Missing '\\x' prefix"));
    }

    #[test]
    fn parse_bytea_hex_wrong_prefix() {
        let result = parse_bytea_hex_string("0x41");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ConversionError));
        assert!(err.to_string().contains("Missing '\\x' prefix"));
    }

    #[test]
    fn parse_bytea_hex_empty_string() {
        let result = parse_bytea_hex_string("");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ConversionError));
        assert!(err.to_string().contains("Missing '\\x' prefix"));
    }

    #[test]
    fn parse_bytea_hex_only_prefix() {
        let result = parse_bytea_hex_string("\\");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ConversionError));
        assert!(err.to_string().contains("Missing '\\x' prefix"));
    }

    #[test]
    fn parse_bytea_hex_odd_length() {
        let result = parse_bytea_hex_string("\\x4");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ConversionError));
        assert!(err.to_string().contains("Odd number of hexadecimal digits"));
    }

    #[test]
    fn parse_bytea_hex_odd_length_multiple() {
        let result = parse_bytea_hex_string("\\x41424");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ConversionError));
        assert!(err.to_string().contains("Odd number of hexadecimal digits"));
    }

    #[test]
    fn parse_bytea_hex_invalid_hex_char() {
        let result = parse_bytea_hex_string("\\x4g");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Invalid hexadecimal digit"));
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

        let result = parse_bytea_hex_string("\\xaéa");
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
