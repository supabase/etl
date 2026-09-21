/// Quotes `value` between `delimiter` characters using ClickHouse escapes.
///
/// ClickHouse applies the same backslash escapes to double-quoted identifiers
/// and single-quoted string literals, so only the delimiter differs.
fn quote_with(delimiter: char, value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push(delimiter);

    for ch in value.chars() {
        match ch {
            '\\' => quoted.push_str("\\\\"),
            '\0' => quoted.push_str("\\0"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            ch if ch == delimiter => {
                quoted.push('\\');
                quoted.push(ch);
            }
            _ => quoted.push(ch),
        }
    }

    quoted.push(delimiter);
    quoted
}

/// Quotes a ClickHouse SQL identifier.
///
/// Embedded double quotes and backslashes are backslash-escaped instead of
/// doubled.
pub(super) fn quote_identifier(identifier: &str) -> String {
    quote_with('"', identifier)
}

/// Quotes a ClickHouse SQL string literal.
///
/// Takes the decoded string value, not a literal from another SQL dialect.
/// PostgreSQL literals must be decoded first because PostgreSQL treats a
/// backslash as an ordinary character while ClickHouse treats it as an escape.
pub(super) fn quote_string_literal(value: &str) -> String {
    quote_with('\'', value)
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

    #[test]
    fn quote_string_literal_escapes_backslashes_and_quotes() {
        // GIVEN: values with escapable characters and a double quote to keep.
        let cases = [
            ("plain", "'plain'"),
            ("it's", "'it\\'s'"),
            ("C:\\temp", "'C:\\\\temp'"),
            ("abc\\", "'abc\\\\'"),
            ("has\"quote", "'has\"quote'"),
            ("line\nbreak", "'line\\nbreak'"),
        ];

        for (value, expected) in cases {
            // WHEN: the value is quoted as a ClickHouse string literal.
            let literal = quote_string_literal(value);

            // THEN: only the delimiter, backslashes, and controls are escaped.
            assert_eq!(literal, expected);
        }
    }
}
