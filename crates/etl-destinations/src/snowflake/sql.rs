/// Quotes a Snowflake SQL identifier.
///
/// Snowflake quoted identifiers are enclosed in double quotes, preserve case,
/// and escape embedded double quotes by writing two double quotes.
pub(super) fn quote_identifier(name: &str) -> String {
    crate::sql::quote_double_identifier(name)
}

/// Quotes a Snowflake SQL string literal.
///
/// Snowflake single-quoted string constants escape single quotes by doubling
/// them and support backslash escape sequences for backslashes and control
/// characters.
pub(super) fn quote_string_literal(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');

    for ch in value.chars() {
        match ch {
            '\'' => quoted.push_str("''"),
            '\\' => quoted.push_str("\\\\"),
            '\0' => quoted.push_str("\\0"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            _ => quoted.push(ch),
        }
    }

    quoted.push('\'');
    quoted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_cases() {
        let cases =
            [("my_table", r#""my_table""#), (r#"my"table"#, r#""my""table""#), ("", r#""""#)];
        for (input, expected) in cases {
            assert_eq!(quote_identifier(input), expected, "input: {input:?}");
        }
    }

    #[test]
    fn quote_string_literal_escapes_single_quotes_and_backslashes() {
        assert_eq!(quote_string_literal(r#"a_b%c\d'e"#), r#"'a_b%c\\d''e'"#);
        assert_eq!(quote_string_literal("line\nnext"), r#"'line\nnext'"#);
    }
}
