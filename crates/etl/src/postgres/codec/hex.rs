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
    let Some(value) = value.as_bytes().strip_prefix(b"\\x") else {
        bail!(
            ErrorKind::ConversionError,
            "Bytea hex string conversion failed",
            "Missing '\\x' prefix"
        );
    };

    let mut result = Vec::with_capacity(value.len() / 2);
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
    fn parse_bytea_hex_accepts_valid_values() {
        let cases: &[(&str, &[u8])] = &[
            ("\\x", &[]),
            ("\\x41", &[0x41]),
            ("\\x48656c6c6f", b"Hello"),
            ("\\x0000", &[0x00, 0x00]),
            ("\\xffff", &[0xff, 0xff]),
            ("\\xaBcD", &[0xab, 0xcd]),
            ("\\x0123456789abcdef", &[0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]),
            ("\\x00010203040506070809", &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        ];

        for (value, expected) in cases {
            assert_eq!(parse_bytea_hex_string(value).unwrap(), *expected, "value: {value:?}");
        }
    }

    #[test]
    fn parse_bytea_hex_rejects_malformed_values() {
        let cases = [
            ("", "Missing '\\x' prefix"),
            ("0x41", "Missing '\\x' prefix"),
            ("\\", "Missing '\\x' prefix"),
            ("aé", "Missing '\\x' prefix"),
            ("\\x4", "Odd number of hexadecimal digits"),
            ("\\x4g", "Invalid hexadecimal digit"),
            ("\\x 1", "Invalid hexadecimal digit"),
            ("\\xaéa", "Invalid hexadecimal digit"),
            ("\\x🤔🤔", "Invalid hexadecimal digit"),
        ];

        for (value, expected_detail) in cases {
            let err = parse_bytea_hex_string(value).unwrap_err();

            assert_eq!(err.kind(), ErrorKind::ConversionError, "value: {value:?}");
            assert_eq!(err.detail(), Some(expected_detail), "value: {value:?}");
        }
    }
}
