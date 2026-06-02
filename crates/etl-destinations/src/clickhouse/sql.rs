/// Quotes a ClickHouse SQL identifier.
///
/// ClickHouse accepts double-quoted identifiers and applies the same escape
/// rules as string literals, so embedded double quotes and backslashes are
/// backslash-escaped instead of doubled.
pub(super) fn quote_identifier(identifier: &str) -> String {
    let mut quoted = String::with_capacity(identifier.len() + 2);
    quoted.push('"');

    for ch in identifier.chars() {
        match ch {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\0' => quoted.push_str("\\0"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            _ => quoted.push(ch),
        }
    }

    quoted.push('"');
    quoted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_escapes_embedded_quotes() {
        assert_eq!(quote_identifier("plain"), "\"plain\"");
        assert_eq!(quote_identifier("has\"quote"), "\"has\\\"quote\"");
        assert_eq!(quote_identifier("path\\name"), "\"path\\\\name\"");
        assert_eq!(quote_identifier("line\nbreak"), "\"line\\nbreak\"");
    }
}
