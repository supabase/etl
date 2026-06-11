use tokio_postgres::types::Type;

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
    /// A SQL timestamp literal.
    TimestampLiteral(String),
    /// A SQL JSON literal.
    JsonLiteral(String),
    /// A UUID v4 generator.
    UuidV4,
    /// The current database user.
    CurrentUser,
    /// The current transaction timestamp.
    CurrentTimestamp,
    /// The current date.
    CurrentDate,
    /// The current time.
    CurrentTime,
    /// The current local timestamp.
    LocalTimestamp,
    /// A Postgres `timezone(..., now())` expression.
    TimezoneNow,
    /// A current temporal expression plus or minus a simple interval.
    IntervalArithmetic {
        /// The temporal expression on the left-hand side.
        base: Box<DefaultExpression>,
        /// The interval arithmetic operator.
        operator: DefaultIntervalOperator,
        /// The simple interval literal.
        interval: DefaultInterval,
        /// The temporal type of the resulting expression.
        temporal_type: DefaultTemporalType,
    },
    /// A portable single-argument literal function.
    LiteralFunction {
        /// The function to evaluate.
        function: DefaultLiteralFunction,
        /// The SQL string literal argument.
        argument: String,
    },
    /// A portable numeric expression made only of numeric literals and
    /// operators.
    NumericExpression(String),
}

/// A simple interval literal used by a default expression.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DefaultInterval {
    /// The interval amount.
    pub amount: i64,
    /// The interval unit.
    pub unit: DefaultIntervalUnit,
    /// The original interval literal without surrounding quotes.
    pub literal: String,
}

/// A supported interval arithmetic operator.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DefaultIntervalOperator {
    /// Addition.
    Add,
    /// Subtraction.
    Subtract,
}

impl DefaultIntervalOperator {
    /// Returns the SQL operator token.
    pub fn as_sql(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Subtract => "-",
        }
    }
}

/// A simple interval unit.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DefaultIntervalUnit {
    /// Microseconds.
    Microsecond,
    /// Milliseconds.
    Millisecond,
    /// Seconds.
    Second,
    /// Minutes.
    Minute,
    /// Hours.
    Hour,
    /// Days.
    Day,
    /// Weeks.
    Week,
    /// Months.
    Month,
    /// Quarters.
    Quarter,
    /// Years.
    Year,
}

impl DefaultIntervalUnit {
    /// Returns the singular uppercase SQL spelling.
    pub fn as_upper_singular(self) -> &'static str {
        match self {
            Self::Microsecond => "MICROSECOND",
            Self::Millisecond => "MILLISECOND",
            Self::Second => "SECOND",
            Self::Minute => "MINUTE",
            Self::Hour => "HOUR",
            Self::Day => "DAY",
            Self::Week => "WEEK",
            Self::Month => "MONTH",
            Self::Quarter => "QUARTER",
            Self::Year => "YEAR",
        }
    }
}

/// A temporal type produced by a default expression.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DefaultTemporalType {
    /// A date expression.
    Date,
    /// A time expression.
    Time,
    /// A timestamp expression.
    Timestamp,
}

/// A supported literal string function.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DefaultLiteralFunction {
    /// Converts a string literal to lowercase.
    Lower,
    /// Converts a string literal to uppercase.
    Upper,
}

impl DefaultLiteralFunction {
    /// Returns the lowercase SQL function name.
    pub fn as_lower_name(self) -> &'static str {
        match self {
            Self::Lower => "lower",
            Self::Upper => "upper",
        }
    }

    /// Returns the uppercase SQL function name.
    pub fn as_upper_name(self) -> &'static str {
        match self {
            Self::Lower => "LOWER",
            Self::Upper => "UPPER",
        }
    }
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

    if is_uuid_expression(expression) {
        return Some(DefaultExpression::UuidV4);
    }

    if is_current_user_expression(expression) {
        return Some(DefaultExpression::CurrentUser);
    }

    if let Some(expression) = parse_current_time_expression(expression, typ) {
        return Some(expression);
    }

    if let Some(expression) = parse_interval_arithmetic_expression(expression, typ) {
        return Some(expression);
    }

    if let Some(expression) = parse_literal_function_expression(expression) {
        return Some(expression);
    }

    if is_unsupported_portability_boundary(expression) {
        return None;
    }

    if is_string_literal(expression) {
        return Some(parse_string_literal(expression, typ));
    }

    if is_numeric_literal(expression) {
        return Some(DefaultExpression::NumericLiteral(expression.to_owned()));
    }

    if is_bool_literal(expression) {
        return Some(DefaultExpression::BooleanLiteral(expression.to_owned()));
    }

    parse_numeric_expression(expression).map(DefaultExpression::NumericExpression)
}

/// Normalizes PostgreSQL-specific expression wrappers.
fn normalize_postgres_expression(expression: &str) -> &str {
    let mut expression = expression.trim();

    loop {
        let stripped = strip_outer_parens(strip_postgres_cast(expression));
        if stripped == expression {
            return expression;
        }

        expression = stripped;
    }
}

/// Strips a trailing Postgres type cast from a default expression.
fn strip_postgres_cast(expression: &str) -> &str {
    let Some(cast_start) = top_level_cast_start(expression) else {
        return expression;
    };

    let type_name = expression[cast_start + 2..].trim();
    let cast_subject = expression[..cast_start].trim();
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
                index += 1;
                while in_string && index < bytes.len() {
                    if bytes[index] == b'\'' {
                        if index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                            index += 2;
                        } else {
                            in_string = false;
                            index += 1;
                        }
                    } else {
                        index += 1;
                    }
                }
            }
            b'(' if !in_string => {
                paren_depth += 1;
                index += 1;
            }
            b')' if !in_string => {
                paren_depth = paren_depth.saturating_sub(1);
                index += 1;
            }
            b':' if !in_string
                && paren_depth == 0
                && index + 1 < bytes.len()
                && bytes[index + 1] == b':' =>
            {
                return Some(index);
            }
            _ => index += 1,
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
                paren_depth += 1;
                index += 1;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                index += 1;
            }
            b'+' | b'-' if paren_depth == 0 && index == 0 => index += 1,
            b'+' | b'-' | b'*' | b'/' | b'%' if paren_depth == 0 => {
                return true;
            }
            b'|' if paren_depth == 0 && index + 1 < bytes.len() && bytes[index + 1] == b'|' => {
                return true;
            }
            _ => index += 1,
        }
    }

    false
}

/// Skips over a SQL single-quoted string literal.
fn skip_string_literal(bytes: &[u8], mut index: usize) -> usize {
    index += 1;

    while index < bytes.len() {
        if bytes[index] == b'\'' {
            if index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                index += 2;
            } else {
                return index + 1;
            }
        } else {
            index += 1;
        }
    }

    index
}

/// Returns whether a string starts with an ASCII prefix, ignoring case.
fn starts_with_ignore_ascii_case(value: &str, prefix: &str) -> bool {
    value
        .as_bytes()
        .get(..prefix.len())
        .is_some_and(|value| value.eq_ignore_ascii_case(prefix.as_bytes()))
}

/// Returns whether a string ends with an ASCII suffix, ignoring case.
fn ends_with_ignore_ascii_case(value: &str, suffix: &str) -> bool {
    value
        .as_bytes()
        .get(value.len().saturating_sub(suffix.len())..)
        .is_some_and(|value| value.eq_ignore_ascii_case(suffix.as_bytes()))
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
                depth += 1;
                index += 1;
            }
            b')' => {
                let Some(new_depth) = depth.checked_sub(1) else {
                    return expression;
                };
                depth = new_depth;
                if depth == 0 && index != bytes.len() - 1 {
                    return expression;
                }
                index += 1;
            }
            _ => index += 1,
        }
    }

    if depth == 0 { expression[1..expression.len() - 1].trim() } else { expression }
}

/// Returns whether an expression is a SQL string literal.
fn is_string_literal(expression: &str) -> bool {
    expression.starts_with('\'') && expression.ends_with('\'')
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

/// Parses string literals, including date/time typed literals.
fn parse_string_literal(expression: &str, typ: &Type) -> DefaultExpression {
    match typ {
        &Type::DATE => DefaultExpression::DateLiteral(expression.to_owned()),
        &Type::TIME => DefaultExpression::TimeLiteral(expression.to_owned()),
        &Type::TIMESTAMP | &Type::TIMESTAMPTZ => {
            DefaultExpression::TimestampLiteral(expression.to_owned())
        }
        &Type::JSON | &Type::JSONB => DefaultExpression::JsonLiteral(expression.to_owned()),
        _ => DefaultExpression::StringLiteral(expression.to_owned()),
    }
}

/// Returns whether the expression is a common UUID generator.
fn is_uuid_expression(expression: &str) -> bool {
    let expression = expression.trim();

    expression.eq_ignore_ascii_case("gen_random_uuid()")
        || expression.eq_ignore_ascii_case("uuid_generate_v4()")
}

/// Returns whether the expression is a current-user expression.
fn is_current_user_expression(expression: &str) -> bool {
    let expression = expression.trim();

    expression.eq_ignore_ascii_case("current_user")
        || expression.eq_ignore_ascii_case("current_user()")
        || expression.eq_ignore_ascii_case("session_user")
}

/// Parses common current-time defaults.
fn parse_current_time_expression(expression: &str, typ: &Type) -> Option<DefaultExpression> {
    let expression = expression.trim();

    if (expression.eq_ignore_ascii_case("now()")
        || expression.eq_ignore_ascii_case("transaction_timestamp()")
        || expression.eq_ignore_ascii_case("current_timestamp")
        || expression.eq_ignore_ascii_case("current_timestamp()"))
        && is_timestamp_type(typ)
    {
        return Some(DefaultExpression::CurrentTimestamp);
    }

    if (expression.eq_ignore_ascii_case("current_date")
        || expression.eq_ignore_ascii_case("current_date()"))
        && *typ == Type::DATE
    {
        return Some(DefaultExpression::CurrentDate);
    }

    if (expression.eq_ignore_ascii_case("current_time")
        || expression.eq_ignore_ascii_case("current_time()"))
        && *typ == Type::TIME
    {
        return Some(DefaultExpression::CurrentTime);
    }

    if (expression.eq_ignore_ascii_case("localtimestamp")
        || expression.eq_ignore_ascii_case("localtimestamp()"))
        && is_timestamp_type(typ)
    {
        return Some(DefaultExpression::LocalTimestamp);
    }

    if starts_with_ignore_ascii_case(expression, "timezone(")
        && ends_with_ignore_ascii_case(expression, "now())")
        && is_timestamp_type(typ)
    {
        return Some(DefaultExpression::TimezoneNow);
    }

    None
}

/// Returns whether a Postgres type maps to a timestamp-like destination type.
fn is_timestamp_type(typ: &Type) -> bool {
    matches!(typ, &Type::TIMESTAMP | &Type::TIMESTAMPTZ)
}

/// Parses current-time plus/minus simple interval defaults.
fn parse_interval_arithmetic_expression(expression: &str, typ: &Type) -> Option<DefaultExpression> {
    let (left, operator, right) = split_top_level_interval_arithmetic(expression)?;
    let base = parse_current_time_expression(left, typ)?;
    let interval = parse_simple_interval_literal(right)?;
    let temporal_type = temporal_type_for_column_type(typ)?;
    let operator = match operator {
        '+' => DefaultIntervalOperator::Add,
        '-' => DefaultIntervalOperator::Subtract,
        _ => return None,
    };

    Some(DefaultExpression::IntervalArithmetic {
        base: Box::new(base),
        operator,
        interval,
        temporal_type,
    })
}

/// Returns the temporal expression type for a Postgres column type.
fn temporal_type_for_column_type(typ: &Type) -> Option<DefaultTemporalType> {
    match typ {
        &Type::DATE => Some(DefaultTemporalType::Date),
        &Type::TIME => Some(DefaultTemporalType::Time),
        &Type::TIMESTAMP | &Type::TIMESTAMPTZ => Some(DefaultTemporalType::Timestamp),
        _ => None,
    }
}

/// Splits `current_time_expression +/- interval '...'`.
fn split_top_level_interval_arithmetic(expression: &str) -> Option<(&str, char, &str)> {
    let bytes = expression.as_bytes();
    let mut index = 0;
    let mut paren_depth: usize = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' => index = skip_string_literal(bytes, index),
            b'(' => {
                paren_depth += 1;
                index += 1;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                index += 1;
            }
            b'+' | b'-' if paren_depth == 0 && index > 0 => {
                let left = expression[..index].trim();
                let right = expression[index + 1..].trim();
                return Some((left, bytes[index] as char, right));
            }
            _ => index += 1,
        }
    }

    None
}

/// Parses an `interval 'N unit'` expression.
fn parse_simple_interval_literal(expression: &str) -> Option<DefaultInterval> {
    let expression = expression.trim();
    let literal = if starts_with_ignore_ascii_case(expression, "interval ") {
        expression[9..].trim()
    } else {
        normalize_postgres_expression(expression)
    };
    if !is_string_literal(literal) {
        return None;
    }

    let literal = &literal[1..literal.len() - 1];
    let mut parts = literal.split_whitespace();
    let amount = parts.next()?.parse::<i64>().ok()?;
    let unit = parse_simple_interval_unit(parts.next()?)?;
    if parts.next().is_some() {
        return None;
    }

    Some(DefaultInterval { amount, unit, literal: literal.to_owned() })
}

/// Parses a simple interval unit.
fn parse_simple_interval_unit(unit: &str) -> Option<DefaultIntervalUnit> {
    let unit = unit.trim_end_matches('s');
    if unit.eq_ignore_ascii_case("microsecond") {
        Some(DefaultIntervalUnit::Microsecond)
    } else if unit.eq_ignore_ascii_case("millisecond") {
        Some(DefaultIntervalUnit::Millisecond)
    } else if unit.eq_ignore_ascii_case("second") {
        Some(DefaultIntervalUnit::Second)
    } else if unit.eq_ignore_ascii_case("minute") {
        Some(DefaultIntervalUnit::Minute)
    } else if unit.eq_ignore_ascii_case("hour") {
        Some(DefaultIntervalUnit::Hour)
    } else if unit.eq_ignore_ascii_case("day") {
        Some(DefaultIntervalUnit::Day)
    } else if unit.eq_ignore_ascii_case("week") {
        Some(DefaultIntervalUnit::Week)
    } else if unit.eq_ignore_ascii_case("month") {
        Some(DefaultIntervalUnit::Month)
    } else if unit.eq_ignore_ascii_case("quarter") {
        Some(DefaultIntervalUnit::Quarter)
    } else if unit.eq_ignore_ascii_case("year") {
        Some(DefaultIntervalUnit::Year)
    } else {
        None
    }
}

/// Parses simple literal functions that are portable across destinations.
fn parse_literal_function_expression(expression: &str) -> Option<DefaultExpression> {
    let (function_name, args) = parse_function_call(expression)?;
    let function = if function_name.eq_ignore_ascii_case("lower") {
        DefaultLiteralFunction::Lower
    } else if function_name.eq_ignore_ascii_case("upper") {
        DefaultLiteralFunction::Upper
    } else {
        return None;
    };

    let arg = normalize_postgres_expression(parse_single_top_level_arg(args)?);
    if !is_string_literal(arg) {
        return None;
    }

    Some(DefaultExpression::LiteralFunction { function, argument: arg.to_owned() })
}

/// Parses a function call that spans the whole expression.
fn parse_function_call(expression: &str) -> Option<(&str, &str)> {
    let open = expression.find('(')?;
    if !expression.ends_with(')') {
        return None;
    }

    let name = expression[..open].trim();
    if name.is_empty() || !name.chars().all(|ch| ch.is_ascii_alphabetic() || ch == '_') {
        return None;
    }

    let bytes = expression.as_bytes();
    let mut index = open;
    let mut depth = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' => index = skip_string_literal(bytes, index),
            b'(' => {
                depth += 1;
                index += 1;
            }
            b')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 && index != bytes.len() - 1 {
                    return None;
                }

                index += 1;
            }
            _ => index += 1,
        }
    }

    if depth == 0 { Some((name, &expression[open + 1..expression.len() - 1])) } else { None }
}

/// Parses a single function argument and rejects top-level comma separators.
fn parse_single_top_level_arg(args: &str) -> Option<&str> {
    let bytes = args.as_bytes();
    let mut index = 0;
    let mut paren_depth: usize = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' => index = skip_string_literal(bytes, index),
            b'(' => {
                paren_depth += 1;
                index += 1;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                index += 1;
            }
            b',' if paren_depth == 0 => return None,
            _ => index += 1,
        }
    }

    Some(args.trim())
}

/// Parses simple literal arithmetic expressions that are broadly portable.
fn parse_numeric_expression(expression: &str) -> Option<String> {
    if is_valid_numeric_expression(expression) { Some(expression.to_owned()) } else { None }
}

/// Returns whether a numeric expression uses only simple arithmetic syntax.
fn is_valid_numeric_expression(expression: &str) -> bool {
    let bytes = expression.as_bytes();
    let mut index = 0;
    let mut paren_depth = 0usize;
    let mut saw_digit = false;
    let mut expect_operand = true;
    let mut unary_sign_allowed = true;

    while index < bytes.len() {
        match bytes[index] {
            byte if byte.is_ascii_whitespace() => index += 1,
            b'+' | b'-' if expect_operand && unary_sign_allowed => {
                unary_sign_allowed = false;
                index += 1;
            }
            b'(' if expect_operand && unary_sign_allowed => {
                paren_depth += 1;
                unary_sign_allowed = true;
                index += 1;
            }
            b'0'..=b'9' | b'.' if expect_operand => {
                let Some(next_index) = scan_numeric_literal(bytes, index) else {
                    return false;
                };
                saw_digit = true;
                index = next_index;
                expect_operand = false;
                unary_sign_allowed = false;
            }
            b'+' | b'-' | b'*' | b'/' | b'%' if !expect_operand => {
                expect_operand = true;
                unary_sign_allowed = true;
                index += 1;
            }
            b')' if !expect_operand && paren_depth > 0 => {
                paren_depth -= 1;
                index += 1;
            }
            _ => return false,
        }
    }

    saw_digit && !expect_operand && paren_depth == 0
}

/// Scans a numeric literal and returns the byte after it.
fn scan_numeric_literal(bytes: &[u8], mut index: usize) -> Option<usize> {
    let mut has_digit = false;
    let mut has_decimal = false;

    while index < bytes.len() {
        match bytes[index] {
            b'0'..=b'9' => {
                has_digit = true;
                index += 1;
            }
            b'.' if !has_decimal => {
                has_decimal = true;
                index += 1;
            }
            _ => break,
        }
    }

    has_digit.then_some(index)
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
    }

    #[test]
    fn parses_current_time_expressions() {
        assert_eq!(
            parse_default_expression("now()", &Type::TIMESTAMPTZ),
            Some(DefaultExpression::CurrentTimestamp)
        );
        assert_eq!(parse_default_expression("now()", &Type::DATE), None);
        assert_eq!(
            parse_default_expression("localtimestamp", &Type::TIMESTAMP),
            Some(DefaultExpression::LocalTimestamp)
        );
        assert_eq!(parse_default_expression("localtimestamp", &Type::DATE), None);
        assert_eq!(
            parse_default_expression("CURRENT_DATE", &Type::DATE),
            Some(DefaultExpression::CurrentDate)
        );
    }

    #[test]
    fn parses_current_time_interval_arithmetic() {
        assert_eq!(
            parse_default_expression("now() + interval '30 days'", &Type::TIMESTAMPTZ),
            Some(DefaultExpression::IntervalArithmetic {
                base: Box::new(DefaultExpression::CurrentTimestamp),
                operator: DefaultIntervalOperator::Add,
                interval: DefaultInterval {
                    amount: 30,
                    unit: DefaultIntervalUnit::Day,
                    literal: "30 days".to_owned(),
                },
                temporal_type: DefaultTemporalType::Timestamp,
            })
        );
        assert_eq!(
            parse_default_expression("(CURRENT_DATE - INTERVAL '7 days')", &Type::DATE),
            Some(DefaultExpression::IntervalArithmetic {
                base: Box::new(DefaultExpression::CurrentDate),
                operator: DefaultIntervalOperator::Subtract,
                interval: DefaultInterval {
                    amount: 7,
                    unit: DefaultIntervalUnit::Day,
                    literal: "7 days".to_owned(),
                },
                temporal_type: DefaultTemporalType::Date,
            })
        );
    }

    #[test]
    fn parses_typed_literals() {
        assert_eq!(
            parse_default_expression("'2026-01-01'::date", &Type::DATE),
            Some(DefaultExpression::DateLiteral("'2026-01-01'".to_owned()))
        );
        assert_eq!(
            parse_default_expression("'{}'::jsonb", &Type::JSONB),
            Some(DefaultExpression::JsonLiteral("'{}'".to_owned()))
        );
    }

    #[test]
    fn parses_common_functions() {
        assert_eq!(
            parse_default_expression("gen_random_uuid()", &Type::UUID),
            Some(DefaultExpression::UuidV4)
        );
        assert_eq!(
            parse_default_expression("CURRENT_USER", &Type::TEXT),
            Some(DefaultExpression::CurrentUser)
        );
    }

    #[test]
    fn parses_literal_string_functions() {
        assert_eq!(
            parse_default_expression("lower('USER'::text)", &Type::TEXT),
            Some(DefaultExpression::LiteralFunction {
                function: DefaultLiteralFunction::Lower,
                argument: "'USER'".to_owned(),
            })
        );
        assert_eq!(
            parse_default_expression("upper('user')", &Type::TEXT),
            Some(DefaultExpression::LiteralFunction {
                function: DefaultLiteralFunction::Upper,
                argument: "'user'".to_owned(),
            })
        );
        assert_eq!(parse_default_expression("lower('a', 'b')", &Type::TEXT), None);
    }

    #[test]
    fn parses_numeric_expressions_conservatively() {
        assert_eq!(
            parse_default_expression("(10 + 5) * 2", &Type::INT4),
            Some(DefaultExpression::NumericExpression("(10 + 5) * 2".to_owned()))
        );
        assert_eq!(parse_default_expression("1 +", &Type::INT4), None);
        assert_eq!(parse_default_expression("1..2", &Type::INT4), None);
        assert_eq!(parse_default_expression("(1 + 2", &Type::INT4), None);
        assert_eq!(parse_default_expression("--1", &Type::INT4), None);
    }

    #[test]
    fn skips_complex_expressions() {
        assert_eq!(parse_default_expression("nextval('users_id_seq')", &Type::INT8), None);
        assert_eq!(
            parse_default_expression("now() + '30 days'::interval", &Type::TIMESTAMPTZ),
            Some(DefaultExpression::IntervalArithmetic {
                base: Box::new(DefaultExpression::CurrentTimestamp),
                operator: DefaultIntervalOperator::Add,
                interval: DefaultInterval {
                    amount: 30,
                    unit: DefaultIntervalUnit::Day,
                    literal: "30 days".to_owned(),
                },
                temporal_type: DefaultTemporalType::Timestamp,
            })
        );
        assert_eq!(parse_default_expression("array['a', 'b']::text[]", &Type::TEXT_ARRAY), None);
    }
}
