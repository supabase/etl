use tokio_postgres::types::Type;
use tracing::warn;

/// A conservative, portable representation of a Postgres column default.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DefaultExpression {
    /// A SQL string literal.
    StringLiteral(String),
    /// A SQL numeric literal.
    NumericLiteral(String),
    /// A SQL boolean literal.
    BooleanLiteral(String),
    /// A SQL date literal.
    DateLiteral(String),
    /// A SQL time literal.
    TimeLiteral(String),
    /// A SQL time with time zone literal.
    TimeTzLiteral(String),
    /// A SQL timestamp literal.
    TimestampLiteral(String),
    /// A SQL timestamp with time zone literal.
    TimestampTzLiteral(String),
    /// A SQL interval literal.
    IntervalLiteral(String),
    /// A SQL JSON literal.
    JsonLiteral(String),
}

/// Parses a Postgres default expression into a portable representation.
///
/// The parser is intentionally conservative. It recognizes values whose
/// meaning is stable across destinations and returns `None` for expressions
/// that need a fuller Postgres evaluator or have destination-specific behavior.
pub fn parse_default_expression(expression: &str, typ: &Type) -> Option<DefaultExpression> {
    let expression = normalize_postgres_expression(expression);
    if expression.is_empty() || expression.eq_ignore_ascii_case("null") {
        return None;
    }

    if is_unsupported_portability_boundary(expression) {
        return None;
    }

    if is_string_literal(expression) {
        return parse_string_literal(expression, typ);
    }

    if is_numeric_literal(expression) {
        return Some(parse_numeric_literal(expression, typ));
    }

    if is_bool_literal(expression) {
        return Some(parse_bool_literal(expression, typ));
    }

    None
}

/// Normalizes PostgreSQL-specific expression wrappers.
fn normalize_postgres_expression(expression: &str) -> &str {
    let mut expression = expression.trim();
    let max_iterations = expression.len();

    for _ in 0..max_iterations {
        let stripped = strip_outer_parens(strip_postgres_cast(expression));
        if stripped == expression {
            return expression;
        }

        if stripped.len() >= expression.len() {
            warn_default_parser_guard(
                "normalization rewrite did not shrink expression",
                expression.len(),
                None,
            );

            return expression;
        }

        expression = stripped;
    }

    warn_default_parser_guard("normalization reached iteration limit", expression.len(), None);

    expression
}

/// Strips a trailing Postgres type cast from a default expression.
fn strip_postgres_cast(expression: &str) -> &str {
    let Some(cast_start) = top_level_cast_start(expression) else {
        return expression;
    };

    let Some(type_start) = cast_start.checked_add(2) else {
        warn_default_parser_guard("cast suffix index overflow", expression.len(), Some(cast_start));

        return expression;
    };

    let Some(type_name) = expression.get(type_start..) else {
        warn_default_parser_guard(
            "cast suffix index out of bounds",
            expression.len(),
            Some(type_start),
        );

        return expression;
    };

    let Some(cast_subject) = expression.get(..cast_start) else {
        warn_default_parser_guard(
            "cast subject index out of bounds",
            expression.len(),
            Some(cast_start),
        );

        return expression;
    };

    let type_name = type_name.trim();
    let cast_subject = cast_subject.trim();
    if is_cast_type_name(type_name) && !has_top_level_binary_operator(cast_subject) {
        cast_subject
    } else {
        expression
    }
}

/// Finds a top-level `::` cast in an expression.
fn top_level_cast_start(expression: &str) -> Option<usize> {
    let bytes = expression.as_bytes();
    let mut index = 0;
    let mut paren_depth: usize = 0;
    let mut in_string = false;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' => {
                in_string = !in_string;
                index = advance_index(index, 1, bytes.len());
                while in_string && index < bytes.len() {
                    if bytes[index] == b'\'' {
                        if next_byte_is(bytes, index, b'\'') {
                            index = advance_index(index, 2, bytes.len());
                        } else {
                            in_string = false;
                            index = advance_index(index, 1, bytes.len());
                        }
                    } else {
                        index = advance_index(index, 1, bytes.len());
                    }
                }
            }
            b'(' if !in_string => {
                let Some(new_depth) = paren_depth.checked_add(1) else {
                    warn_default_parser_guard(
                        "cast parser parenthesis depth overflow",
                        bytes.len(),
                        Some(index),
                    );

                    return None;
                };
                paren_depth = new_depth;
                index = advance_index(index, 1, bytes.len());
            }
            b')' if !in_string => {
                paren_depth = paren_depth.saturating_sub(1);
                index = advance_index(index, 1, bytes.len());
            }
            b':' if !in_string && paren_depth == 0 && next_byte_is(bytes, index, b':') => {
                return Some(index);
            }
            _ => index = advance_index(index, 1, bytes.len()),
        }
    }

    None
}

/// Returns whether a cast suffix looks like a plain Postgres type name.
fn is_cast_type_name(type_name: &str) -> bool {
    !type_name.is_empty()
        && type_name.chars().all(|ch| {
            ch.is_ascii_alphanumeric()
                || matches!(ch, '_' | ' ' | '"' | '.' | '[' | ']' | '(' | ')' | ',')
        })
}

/// Returns whether a Postgres type is text-like.
fn is_text_type(typ: &Type) -> bool {
    matches!(typ, &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT)
}

/// Returns whether a Postgres type is numeric-like.
fn is_numeric_type(typ: &Type) -> bool {
    matches!(
        typ,
        &Type::INT2
            | &Type::INT4
            | &Type::INT8
            | &Type::FLOAT4
            | &Type::FLOAT8
            | &Type::NUMERIC
            | &Type::OID
    )
}

/// Returns a SQL string literal for a parser-validated simple literal.
fn quote_simple_string_literal(expression: &str) -> String {
    format!("'{expression}'")
}

/// Unquotes a SQL single-quoted string literal.
fn unquote_string_literal(expression: &str) -> Option<String> {
    if !is_string_literal(expression) {
        return None;
    }

    let end = expression.len().checked_sub(1)?;
    Some(expression.get(1..end)?.replace("''", "'"))
}

/// Returns whether an expression contains a top-level binary operator.
fn has_top_level_binary_operator(expression: &str) -> bool {
    let bytes = expression.as_bytes();
    let mut index = 0;
    let mut paren_depth: usize = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' => {
                index = skip_string_literal(bytes, index);
            }
            b'(' => {
                let Some(new_depth) = paren_depth.checked_add(1) else {
                    warn_default_parser_guard(
                        "binary operator parser parenthesis depth overflow",
                        bytes.len(),
                        Some(index),
                    );

                    return true;
                };
                paren_depth = new_depth;
                index = advance_index(index, 1, bytes.len());
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                index = advance_index(index, 1, bytes.len());
            }
            b'+' | b'-' if paren_depth == 0 && index == 0 => {
                index = advance_index(index, 1, bytes.len());
            }
            b'+' | b'-' | b'*' | b'/' | b'%' if paren_depth == 0 => {
                return true;
            }
            b'|' if paren_depth == 0 && next_byte_is(bytes, index, b'|') => {
                return true;
            }
            _ => index = advance_index(index, 1, bytes.len()),
        }
    }

    false
}

/// Skips over a SQL single-quoted string literal.
fn skip_string_literal(bytes: &[u8], index: usize) -> usize {
    let Some(end) = string_literal_end(bytes, index) else {
        return bytes.len();
    };

    end
}

/// Returns the byte after a SQL single-quoted string literal.
fn string_literal_end(bytes: &[u8], mut index: usize) -> Option<usize> {
    if bytes.get(index) != Some(&b'\'') {
        return None;
    }

    index = advance_index(index, 1, bytes.len());

    while index < bytes.len() {
        if bytes[index] == b'\'' {
            if next_byte_is(bytes, index, b'\'') {
                index = advance_index(index, 2, bytes.len());
            } else {
                return Some(advance_index(index, 1, bytes.len()));
            }
        } else {
            index = advance_index(index, 1, bytes.len());
        }
    }

    None
}

/// Advances a byte index, capping at the input length on overflow.
fn advance_index(index: usize, amount: usize, len: usize) -> usize {
    index.checked_add(amount).unwrap_or(len).min(len)
}

/// Returns whether the byte after `index` equals `expected`.
fn next_byte_is(bytes: &[u8], index: usize, expected: u8) -> bool {
    index
        .checked_add(1)
        .and_then(|next_index| bytes.get(next_index))
        .is_some_and(|byte| *byte == expected)
}

/// Logs a defensive parser guard without including source default contents.
fn warn_default_parser_guard(
    reason: &'static str,
    expression_len: usize,
    byte_index: Option<usize>,
) {
    if let Some(byte_index) = byte_index {
        warn!(reason, expression_len, byte_index, "default expression parser hit defensive guard");
    } else {
        warn!(reason, expression_len, "default expression parser hit defensive guard");
    }
}

/// Returns whether a string starts with an ASCII prefix, ignoring case.
fn starts_with_ignore_ascii_case(value: &str, prefix: &str) -> bool {
    value
        .as_bytes()
        .get(..prefix.len())
        .is_some_and(|value| value.eq_ignore_ascii_case(prefix.as_bytes()))
}

/// Returns whether a string contains an ASCII needle, ignoring case.
fn contains_ignore_ascii_case(value: &str, needle: &str) -> bool {
    let needle = needle.as_bytes();
    !needle.is_empty()
        && value.as_bytes().windows(needle.len()).any(|window| window.eq_ignore_ascii_case(needle))
}

/// Strips one pair of wrapping parentheses around an entire expression.
fn strip_outer_parens(expression: &str) -> &str {
    let expression = expression.trim();
    if !expression.starts_with('(') || !expression.ends_with(')') {
        return expression;
    }

    let bytes = expression.as_bytes();
    let mut depth = 0usize;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' => {
                index = skip_string_literal(bytes, index);
            }
            b'(' => {
                let Some(new_depth) = depth.checked_add(1) else {
                    warn_default_parser_guard(
                        "outer parenthesis parser depth overflow",
                        bytes.len(),
                        Some(index),
                    );

                    return expression;
                };
                depth = new_depth;
                index = advance_index(index, 1, bytes.len());
            }
            b')' => {
                let Some(new_depth) = depth.checked_sub(1) else {
                    warn_default_parser_guard(
                        "outer parenthesis parser depth underflow",
                        bytes.len(),
                        Some(index),
                    );

                    return expression;
                };
                depth = new_depth;
                if depth == 0 && index != bytes.len().saturating_sub(1) {
                    return expression;
                }
                index = advance_index(index, 1, bytes.len());
            }
            _ => index = advance_index(index, 1, bytes.len()),
        }
    }

    if depth != 0 {
        return expression;
    }

    let Some(end) = expression.len().checked_sub(1) else {
        return expression;
    };

    expression.get(1..end).map_or(expression, str::trim)
}

/// Returns whether an expression is exactly one SQL string literal.
fn is_string_literal(expression: &str) -> bool {
    string_literal_end(expression.as_bytes(), 0).is_some_and(|end| end == expression.len())
}

/// Returns whether an expression is a numeric SQL literal.
fn is_numeric_literal(expression: &str) -> bool {
    let mut chars = expression.chars().peekable();
    if matches!(chars.peek(), Some('+' | '-')) {
        chars.next();
    }

    let mut has_digit = false;
    let mut has_decimal = false;
    for ch in chars {
        if ch.is_ascii_digit() {
            has_digit = true;
        } else if ch == '.' && !has_decimal {
            has_decimal = true;
        } else {
            return false;
        }
    }

    has_digit
}

/// Returns whether an expression is a SQL boolean literal.
fn is_bool_literal(expression: &str) -> bool {
    expression.eq_ignore_ascii_case("true") || expression.eq_ignore_ascii_case("false")
}

/// Returns whether an expression crosses a known portability boundary.
fn is_unsupported_portability_boundary(expression: &str) -> bool {
    starts_with_ignore_ascii_case(expression, "nextval(")
        || contains_ignore_ascii_case(expression, "select ")
        || expression.contains("::")
        || starts_with_ignore_ascii_case(expression, "array[")
        || starts_with_ignore_ascii_case(expression, "array ")
}

/// Parses string literals, including type-shaped literals.
fn parse_string_literal(expression: &str, typ: &Type) -> Option<DefaultExpression> {
    match typ {
        &Type::BOOL => {
            let expression = unquote_string_literal(expression)?;
            is_bool_literal(&expression).then_some(DefaultExpression::BooleanLiteral(expression))
        }
        typ if is_numeric_type(typ) => {
            let expression = unquote_string_literal(expression)?;
            is_numeric_literal(&expression).then_some(DefaultExpression::NumericLiteral(expression))
        }
        &Type::DATE => Some(DefaultExpression::DateLiteral(expression.to_owned())),
        &Type::TIME => Some(DefaultExpression::TimeLiteral(expression.to_owned())),
        &Type::TIMETZ => Some(DefaultExpression::TimeTzLiteral(expression.to_owned())),
        &Type::TIMESTAMP => Some(DefaultExpression::TimestampLiteral(expression.to_owned())),
        &Type::TIMESTAMPTZ => Some(DefaultExpression::TimestampTzLiteral(expression.to_owned())),
        &Type::INTERVAL => Some(DefaultExpression::IntervalLiteral(expression.to_owned())),
        &Type::JSON | &Type::JSONB => Some(DefaultExpression::JsonLiteral(expression.to_owned())),
        _ => Some(DefaultExpression::StringLiteral(expression.to_owned())),
    }
}

/// Parses numeric literals, using text-like column types to disambiguate.
fn parse_numeric_literal(expression: &str, typ: &Type) -> DefaultExpression {
    if is_text_type(typ) {
        DefaultExpression::StringLiteral(quote_simple_string_literal(expression))
    } else {
        DefaultExpression::NumericLiteral(expression.to_owned())
    }
}

/// Parses boolean literals, using text-like column types to disambiguate.
fn parse_bool_literal(expression: &str, typ: &Type) -> DefaultExpression {
    if is_text_type(typ) {
        DefaultExpression::StringLiteral(quote_simple_string_literal(expression))
    } else {
        DefaultExpression::BooleanLiteral(expression.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_literals() {
        assert_eq!(
            parse_default_expression("'pending'::text", &Type::TEXT),
            Some(DefaultExpression::StringLiteral("'pending'".to_owned()))
        );
        assert_eq!(
            parse_default_expression("('don''t'::text)", &Type::TEXT),
            Some(DefaultExpression::StringLiteral("'don''t'".to_owned()))
        );
        assert_eq!(
            parse_default_expression("42", &Type::INT4),
            Some(DefaultExpression::NumericLiteral("42".to_owned()))
        );
        assert_eq!(
            parse_default_expression("false", &Type::BOOL),
            Some(DefaultExpression::BooleanLiteral("false".to_owned()))
        );
        assert_eq!(
            parse_default_expression("true::text", &Type::TEXT),
            Some(DefaultExpression::StringLiteral("'true'".to_owned()))
        );
        assert_eq!(
            parse_default_expression("42::text", &Type::TEXT),
            Some(DefaultExpression::StringLiteral("'42'".to_owned()))
        );
        assert_eq!(
            parse_default_expression("'true'::boolean", &Type::BOOL),
            Some(DefaultExpression::BooleanLiteral("true".to_owned()))
        );
        assert_eq!(
            parse_default_expression("'42'::integer", &Type::INT4),
            Some(DefaultExpression::NumericLiteral("42".to_owned()))
        );
        assert_eq!(
            parse_default_expression("'42.10'::numeric(10,2)", &Type::NUMERIC),
            Some(DefaultExpression::NumericLiteral("42.10".to_owned()))
        );
        assert_eq!(
            parse_default_expression("'1'::integer", &Type::INT4),
            Some(DefaultExpression::NumericLiteral("1".to_owned()))
        );
        assert_eq!(parse_default_expression("'abc'::text", &Type::INT4), None);
    }

    #[test]
    fn normalizes_nested_postgres_wrappers() {
        assert_eq!(normalize_postgres_expression("((('pending'::text)))"), "'pending'");
    }

    #[test]
    fn parses_typed_literals() {
        assert_eq!(
            parse_default_expression("'2026-01-01'::date", &Type::DATE),
            Some(DefaultExpression::DateLiteral("'2026-01-01'".to_owned()))
        );
        assert_eq!(
            parse_default_expression("'12:30:00'::time without time zone", &Type::TIME),
            Some(DefaultExpression::TimeLiteral("'12:30:00'".to_owned()))
        );
        assert_eq!(
            parse_default_expression("'12:30:00+02'::time with time zone", &Type::TIMETZ),
            Some(DefaultExpression::TimeTzLiteral("'12:30:00+02'".to_owned()))
        );
        assert_eq!(
            parse_default_expression("'2026-01-01 12:30:00'::timestamp", &Type::TIMESTAMP),
            Some(DefaultExpression::TimestampLiteral("'2026-01-01 12:30:00'".to_owned()))
        );
        assert_eq!(
            parse_default_expression("'2026-01-01 12:30:00+02'::timestamptz", &Type::TIMESTAMPTZ),
            Some(DefaultExpression::TimestampTzLiteral("'2026-01-01 12:30:00+02'".to_owned(),))
        );
        assert_eq!(
            parse_default_expression("'30 days'::interval", &Type::INTERVAL),
            Some(DefaultExpression::IntervalLiteral("'30 days'".to_owned()))
        );
        assert_eq!(
            parse_default_expression("'{}'::jsonb", &Type::JSONB),
            Some(DefaultExpression::JsonLiteral("'{}'".to_owned()))
        );
        assert_eq!(
            parse_default_expression("'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid", &Type::UUID,),
            Some(DefaultExpression::StringLiteral(
                "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'".to_owned(),
            ))
        );
    }

    #[test]
    fn skips_non_literal_expressions() {
        assert_eq!(parse_default_expression("now()", &Type::TIMESTAMPTZ), None);
        assert_eq!(parse_default_expression("current_timestamp", &Type::TIMESTAMPTZ), None);
        assert_eq!(parse_default_expression("current_date", &Type::DATE), None);
        assert_eq!(parse_default_expression("current_time", &Type::TIMETZ), None);
        assert_eq!(parse_default_expression("localtime", &Type::TIME), None);
        assert_eq!(parse_default_expression("localtimestamp", &Type::TIMESTAMP), None);
        assert_eq!(parse_default_expression("timezone('UTC', now())", &Type::TIMESTAMP), None);
        assert_eq!(parse_default_expression("gen_random_uuid()", &Type::UUID), None);
        assert_eq!(parse_default_expression("uuid_generate_v4()", &Type::UUID), None);
        assert_eq!(parse_default_expression("current_user", &Type::TEXT), None);
        assert_eq!(parse_default_expression("session_user", &Type::TEXT), None);
        assert_eq!(parse_default_expression("lower('USER'::text)", &Type::TEXT), None);
        assert_eq!(parse_default_expression("upper('user')", &Type::TEXT), None);
        assert_eq!(parse_default_expression("'a' || 'b'", &Type::TEXT), None);
        assert_eq!(
            parse_default_expression("(('user'::text || '_'::text) || 'id'::text)", &Type::TEXT),
            None
        );
        assert_eq!(parse_default_expression("lower('a' || 'b')", &Type::TEXT), None);
        assert_eq!(parse_default_expression("(10 + 5) * 2", &Type::INT4), None);
        assert_eq!(parse_default_expression("1 +", &Type::INT4), None);
        assert_eq!(parse_default_expression("1..2", &Type::INT4), None);
        assert_eq!(parse_default_expression("(1 + 2", &Type::INT4), None);
        assert_eq!(parse_default_expression("--1", &Type::INT4), None);
        assert_eq!(parse_default_expression("nextval('users_id_seq')", &Type::INT8), None);
        assert_eq!(
            parse_default_expression("now() + '30 days'::interval", &Type::TIMESTAMPTZ),
            None
        );
        assert_eq!(parse_default_expression("array['a', 'b']::text[]", &Type::TEXT_ARRAY), None);
        assert_eq!(parse_default_expression("1e6", &Type::INT8), None);

        for expression in ["'", "('", "(('a'", "'a", "'a'::", "((1 + 2"] {
            assert_eq!(parse_default_expression(expression, &Type::TEXT), None);
        }
    }
}
