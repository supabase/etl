use crate::bail;
use crate::error::ErrorKind;
use crate::error::EtlResult;

/// Parses a Postgres boolean value from its text format representation.
///
/// Postgres represents boolean values in text format as single characters:
/// - `"t"` → `true` (exactly one lowercase 't')
/// - `"f"` → `false` (exactly one lowercase 'f')
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
