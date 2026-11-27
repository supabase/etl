use crate::{
    bail,
    error::{ErrorKind, EtlResult},
};

/// Marks input bytes outside the ASCII hexadecimal alphabet.
const INVALID_DIGIT: u8 = u8::MAX;

/// Maps each input byte to a nibble or [`INVALID_DIGIT`].
const HEX_DIGITS: [u8; 256] = {
    let mut digits = [INVALID_DIGIT; 256];
    let mut nibble = 0_u8;
    while nibble < 16 {
        let lower = b"0123456789abcdef"[nibble as usize];
        digits[lower as usize] = nibble;
        digits[lower.to_ascii_uppercase() as usize] = nibble;
        nibble += 1;
    }
    digits
};

/// Converts a Postgres bytea hex string to a byte array.
///
/// This function parses Postgres's hex-encoded bytea format, which uses the
/// `\x` prefix followed by hexadecimal digits. Each pair of hex digits
/// represents one byte in the output array.
pub(crate) fn parse_bytea_hex_string(value: &str) -> EtlResult<Vec<u8>> {
    let Some(value) = value.as_bytes().strip_prefix(b"\\x") else {
        bail!(
            ErrorKind::ConversionError,
            "Bytea hex string conversion failed",
            "Missing '\\x' prefix"
        );
    };

    if !value.len().is_multiple_of(2) {
        bail!(
            ErrorKind::ConversionError,
            "Bytea hex string conversion failed",
            "Odd number of hexadecimal digits"
        );
    }

    let mut result = Vec::with_capacity(value.len() / 2);
    for digits in value.chunks_exact(2) {
        let high = HEX_DIGITS[usize::from(digits[0])];
        let low = HEX_DIGITS[usize::from(digits[1])];
        if (high | low) == INVALID_DIGIT {
            bail!(
                ErrorKind::ConversionError,
                "Bytea hex string conversion failed",
                "Invalid hexadecimal digit"
            );
        }

        result.push((high << 4) | low);
    }

    Ok(result)
}

#[cfg(kani)]
mod verification;

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
    fn hex_digits_match_the_ascii_hex_alphabet() {
        for byte in u8::MIN..=u8::MAX {
            let expected = char::from(byte).to_digit(16);
            let digit = HEX_DIGITS[usize::from(byte)];

            assert_eq!((digit != INVALID_DIGIT).then_some(u32::from(digit)), expected);
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
