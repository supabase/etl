use crate::{
    bail,
    error::{ErrorKind, EtlResult},
};

/// Parses a Postgres boolean value from its text format representation.
///
/// Postgres represents boolean values in text format as single characters:
/// - `"t"` → `true` (exactly one lowercase 't')
/// - `"f"` → `false` (exactly one lowercase 'f')
pub(crate) fn parse_bool(s: &str) -> EtlResult<bool> {
    if s == "t" {
        Ok(true)
    } else if s == "f" {
        Ok(false)
    } else {
        bail!(ErrorKind::InvalidData, "Invalid boolean value", "Boolean value must be 't' or 'f'");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    #[test]
    fn parse_bool_accepts_postgres_text_values() {
        assert!(parse_bool("t").unwrap());
        assert!(!parse_bool("f").unwrap());
    }

    #[test]
    fn parse_bool_rejects_other_values_with_a_generic_error() {
        let invalid_values = [
            "", "true", "false", "0", "1", "T", "F", " t", "t ", " f ", "t\n", "f\t", "t\0", "🤔",
            "ÿ", "tt", "tf", "ft", "ff",
        ];

        for value in invalid_values {
            let err = parse_bool(value).unwrap_err();

            assert_eq!(err.kind(), ErrorKind::InvalidData, "value: {value:?}");
            assert_eq!(err.description(), Some("Invalid boolean value"), "value: {value:?}");
            assert_eq!(err.detail(), Some("Boolean value must be 't' or 'f'"), "value: {value:?}");
        }
    }
}
