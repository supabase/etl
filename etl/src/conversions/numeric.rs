use byteorder::{BigEndian, ReadBytesExt};
use std::{
    io::Cursor,
    iter::Peekable,
    str::{Chars, FromStr},
};
use tokio_postgres::types::{FromSql, IsNull, ToSql, Type};

const POSITIVE_SIGN: u16 = 0x0000;
const NEGATIVE_SIGN: u16 = 0x4000;
const NAN_SIGN: u16 = 0xC000;
const POSITIVE_INFINITY_SIGN: u16 = 0xC000;
const NEGATIVE_INFINITY_SIGN: u16 = 0xF000;

/// Sign indicator for PostgreSQL numeric values.
///
/// [`Sign`] represents whether a numeric value is positive or negative,
/// used internally in the PostgreSQL numeric wire format representation.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub enum Sign {
    /// Positive numeric value.
    Positive,
    /// Negative numeric value.
    Negative,
}

/// Error indicating an invalid sign value in PostgreSQL numeric format.
///
/// [`InvalidSign`] wraps the invalid sign value encountered when parsing
/// numeric data from PostgreSQL's wire format.
pub struct InvalidSign(u16);

impl TryFrom<u16> for Sign {
    type Error = InvalidSign;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Ok(match value {
            0x0000 => Sign::Positive,
            0x4000 => Sign::Negative,
            sign => return Err(InvalidSign(sign)),
        })
    }
}

impl From<Sign> for u16 {
    fn from(value: Sign) -> Self {
        match value {
            Sign::Positive => POSITIVE_SIGN,
            Sign::Negative => NEGATIVE_SIGN,
        }
    }
}

/// PostgreSQL NUMERIC/DECIMAL type with arbitrary precision.
///
/// [`PgNumeric`] represents PostgreSQL's NUMERIC and DECIMAL types, which support
/// arbitrary precision arithmetic. This enum closely matches PostgreSQL's internal
/// wire format and can represent special values like NaN and infinity.
///
/// The numeric format uses base-10000 digits internally for efficient storage
/// and calculation while maintaining exact decimal precision.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub enum PgNumeric {
    /// Not a number (NaN) - result of invalid operations.
    NaN,
    /// Positive infinity - result of overflow in a positive direction.
    PositiveInfinity,
    /// Negative infinity - result of overflow in a negative direction.
    NegativeInfinity,
    /// Regular numeric value with arbitrary precision.
    Value {
        /// Sign of the numeric value.
        sign: Sign,
        /// Weight represents the power of 10000 for the first digit.
        /// For example, if weight=2, the first digit represents multiples of 10000^2.
        weight: i16,
        /// Number of decimal digits after the decimal point for display purposes.
        scale: u16,
        /// Actual numeric digits stored in base-10000 format for efficiency.
        digits: Vec<i16>,
    },
}

const ZERO: PgNumeric = PgNumeric::Value {
    sign: Sign::Positive,
    weight: 0,
    scale: 0,
    digits: vec![],
};

impl Default for PgNumeric {
    fn default() -> Self {
        ZERO
    }
}

impl FromStr for PgNumeric {
    type Err = ParseNumericError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut chars = input.chars().peekable();

        // Skip leading spaces
        skip_whitespace(&mut chars);

        if chars.peek().is_none() {
            return Err(ParseNumericError::InvalidSyntax);
        }

        // Handle sign
        let sign = match chars.peek() {
            Some('+') => {
                chars.next();
                Sign::Positive
            }
            Some('-') => {
                chars.next();
                Sign::Negative
            }
            _ => Sign::Positive,
        };

        // Check for special values (NaN, infinity)
        if !matches!(chars.peek(), Some('0'..='9') | Some('.')) {
            return parse_special_value(&mut chars, &sign);
        }

        // Parse regular numeric value
        parse_numeric_value(&mut chars, sign)
    }
}

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(
        _: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Sync + Send>> {
        let mut rdr = Cursor::new(raw);

        let num_digits = rdr.read_u16::<BigEndian>()?;

        let weight = rdr.read_i16::<BigEndian>()?;

        let sign = rdr.read_u16::<BigEndian>()?;
        let sign: Sign = match sign.try_into() {
            Ok(sign) => sign,
            Err(InvalidSign(0xC000)) => return Ok(PgNumeric::NaN),
            Err(InvalidSign(0xD000)) => return Ok(PgNumeric::PositiveInfinity),
            Err(InvalidSign(0xF000)) => return Ok(PgNumeric::NegativeInfinity),
            Err(InvalidSign(v)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid sign {v:#04x}"),
                )
                .into());
            }
        };

        let scale = rdr.read_u16::<BigEndian>()?;

        let mut digits = Vec::with_capacity(num_digits as usize);
        for _ in 0..num_digits {
            digits.push(rdr.read_i16::<BigEndian>()?);
        }

        Ok(PgNumeric::Value {
            sign,
            weight,
            scale,
            digits,
        })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
}

impl ToSql for PgNumeric {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let (sign, weight, scale, digits) = match self {
            PgNumeric::NaN => (NAN_SIGN, &0i16, &0u16, &vec![]),
            PgNumeric::PositiveInfinity => (POSITIVE_INFINITY_SIGN, &0i16, &0u16, &vec![]),
            PgNumeric::NegativeInfinity => (NEGATIVE_INFINITY_SIGN, &0i16, &0u16, &vec![]),
            PgNumeric::Value {
                sign: Sign::Positive,
                weight,
                scale,
                digits,
            } => (POSITIVE_SIGN, weight, scale, digits),
            PgNumeric::Value {
                sign: Sign::Negative,
                weight,
                scale,
                digits,
            } => (NEGATIVE_SIGN, weight, scale, digits),
        };

        let num_digits: u16 = digits.len().try_into()?;
        out.extend_from_slice(&num_digits.to_be_bytes());
        out.extend_from_slice(&weight.to_be_bytes());
        out.extend_from_slice(&sign.to_be_bytes());
        out.extend_from_slice(&scale.to_be_bytes());

        for digit in digits {
            out.extend_from_slice(&digit.to_be_bytes());
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }

    tokio_postgres::types::to_sql_checked!();
}

/// Error types that can occur when parsing numeric strings.
///
/// [`ParseNumericError`] provides specific error categories for numeric parsing
/// failures, enabling appropriate error handling and user feedback.
#[derive(Debug, Clone, PartialEq)]
pub enum ParseNumericError {
    /// The input string has invalid numeric syntax.
    InvalidSyntax,
    /// The numeric value is outside the representable range.
    ValueOutOfRange,
}

/// Skips whitespace characters from a peekable character iterator.
///
/// This helper function advances the iterator past all consecutive whitespace
/// characters, stopping at the first non-whitespace character or end of input.
fn skip_whitespace(chars: &mut Peekable<Chars>) {
    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() {
            chars.next();
        } else {
            break;
        }
    }
}

impl std::fmt::Display for ParseNumericError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseNumericError::InvalidSyntax => write!(f, "Invalid syntax"),
            ParseNumericError::ValueOutOfRange => write!(f, "Value out of range"),
        }
    }
}

impl std::error::Error for ParseNumericError {}

/// Parses special numeric values like NaN, Infinity, and -Infinity.
///
/// This function handles PostgreSQL's special numeric values when they appear
/// in string format. NaN cannot have a negative sign, while infinity values
/// respect the sign parameter.
fn parse_special_value(
    chars: &mut Peekable<Chars>,
    sign: &Sign,
) -> Result<PgNumeric, ParseNumericError> {
    let remaining: String = chars.collect();
    let remaining_lower = remaining.to_lowercase();

    if remaining_lower == "nan" {
        // NaN must not have a sign
        if matches!(sign, Sign::Negative) {
            return Err(ParseNumericError::InvalidSyntax);
        }
        Ok(PgNumeric::NaN)
    } else if remaining_lower == "infinity" || remaining_lower == "inf" {
        match sign {
            Sign::Positive => Ok(PgNumeric::PositiveInfinity),
            Sign::Negative => Ok(PgNumeric::NegativeInfinity),
        }
    } else {
        Err(ParseNumericError::InvalidSyntax)
    }
}

/// Parses a regular numeric value from character input.
///
/// This function processes decimal digits, decimal points, underscores (digit
/// separators), and scientific notation to construct a numeric value. It handles
/// both integer and fractional parts with arbitrary precision.
fn parse_numeric_value(
    chars: &mut Peekable<Chars>,
    sign: Sign,
) -> Result<PgNumeric, ParseNumericError> {
    let mut decimal_digits = Vec::new();
    let mut have_decimal_point = false;
    let mut dweight = -1i32; // Decimal weight (number of digits before decimal point - 1)
    let mut dscale = 0u32; // Number of digits after decimal point

    // Check for initial decimal point
    if chars.peek() == Some(&'.') {
        have_decimal_point = true;
        chars.next();
    }

    // Must have at least one digit
    if !matches!(chars.peek(), Some('0'..='9')) {
        return Err(ParseNumericError::InvalidSyntax);
    }

    // Parse digits and decimal point
    while let Some(&ch) = chars.peek() {
        match ch {
            '0'..='9' => {
                chars.next();
                decimal_digits.push(ch as u8 - b'0');
                if !have_decimal_point {
                    dweight += 1;
                } else {
                    dscale += 1;
                }
            }
            '.' => {
                if have_decimal_point {
                    return Err(ParseNumericError::InvalidSyntax);
                }
                have_decimal_point = true;
                chars.next();
                // Decimal point must not be followed by underscore
                if chars.peek() == Some(&'_') {
                    return Err(ParseNumericError::InvalidSyntax);
                }
            }
            '_' => {
                chars.next();
                // Underscore must be followed by more digits
                if !matches!(chars.peek(), Some('0'..='9')) {
                    return Err(ParseNumericError::InvalidSyntax);
                }
            }
            _ => break,
        }
    }

    // Handle scientific notation
    if matches!(chars.peek(), Some('e') | Some('E')) {
        chars.next();
        let mut exponent = 0i64;
        let mut exp_negative = false;

        // Handle exponent sign
        match chars.peek() {
            Some('+') => {
                chars.next();
            }
            Some('-') => {
                exp_negative = true;
                chars.next();
            }
            _ => {}
        }

        // Parse exponent digits
        if !matches!(chars.peek(), Some('0'..='9')) {
            return Err(ParseNumericError::InvalidSyntax);
        }

        while let Some(&ch) = chars.peek() {
            match ch {
                '0'..='9' => {
                    chars.next();
                    exponent = exponent * 10 + (ch as u8 - b'0') as i64;
                    if exponent > i32::MAX as i64 / 2 {
                        return Err(ParseNumericError::ValueOutOfRange);
                    }
                }
                '_' => {
                    chars.next();
                    if !matches!(chars.peek(), Some('0'..='9')) {
                        return Err(ParseNumericError::InvalidSyntax);
                    }
                }
                _ => break,
            }
        }

        if exp_negative {
            exponent = -exponent;
        }

        dweight += exponent as i32;
        dscale = if (dscale as i64 - exponent) < 0 {
            0
        } else {
            (dscale as i64 - exponent) as u32
        };
    }

    // Check for trailing whitespace/junk
    skip_whitespace(chars);
    if chars.peek().is_some() {
        return Err(ParseNumericError::InvalidSyntax);
    }

    // Convert to base-10000 representation
    convert_to_base_10000(decimal_digits, dweight, dscale as u16, sign)
}

/// Converts decimal digits to PostgreSQL's base-10000 internal format.
///
/// This function transforms a sequence of decimal digits into PostgreSQL's
/// efficient base-10000 representation, where each internal digit represents
/// up to 10000 in decimal. This format balances storage efficiency with
/// calculation performance.
fn convert_to_base_10000(
    decimal_digits: Vec<u8>,
    dweight: i32,
    dscale: u16,
    sign: Sign,
) -> Result<PgNumeric, ParseNumericError> {
    if decimal_digits.is_empty() {
        return Ok(PgNumeric::Value {
            sign: Sign::Positive,
            weight: 0,
            scale: dscale,
            digits: vec![],
        });
    }

    // Calculate weight in base-10000 terms
    // Each base-10000 digit represents 4 decimal digits
    let weight = if dweight >= 0 {
        (dweight + 4) / 4 - 1
    } else {
        -((-dweight - 1) / 4 + 1)
    };

    // Calculate offset for proper alignment
    let offset = (weight + 1) * 4 - (dweight + 1);
    let total_decimal_digits = decimal_digits.len() as i32 + offset;
    let ndigits = (total_decimal_digits + 3) / 4; // Round up to nearest multiple of 4

    // Create padded decimal digits array
    let mut padded_digits = vec![0u8; ndigits as usize * 4];
    let start_idx = offset as usize;

    for (i, &digit) in decimal_digits.iter().enumerate() {
        if start_idx + i < padded_digits.len() {
            padded_digits[start_idx + i] = digit;
        }
    }

    // Convert groups of 4 decimal digits to base-10000 digits
    let mut base_10000_digits = Vec::new();
    for chunk in padded_digits.chunks(4) {
        let mut digit = 0i16;
        for &d in chunk {
            digit = digit * 10 + d as i16;
        }
        base_10000_digits.push(digit);
    }

    // Strip leading and trailing zeros
    strip_leading_zeros(&mut base_10000_digits);
    strip_trailing_zeros(&mut base_10000_digits);

    // Adjust weight if we stripped leading zeros
    let leading_zeros_stripped = ndigits - base_10000_digits.len() as i32;
    let final_weight = weight - leading_zeros_stripped;

    Ok(PgNumeric::Value {
        sign,
        weight: final_weight as i16,
        scale: dscale,
        digits: base_10000_digits,
    })
}

/// Removes leading zero digits from a base-10000 digit sequence.
///
/// This function strips unnecessary leading zeros while preserving at least
/// one digit to represent zero values correctly.
fn strip_leading_zeros(digits: &mut Vec<i16>) {
    while let Some(&first) = digits.first() {
        if first == 0 && digits.len() > 1 {
            digits.remove(0);
        } else {
            break;
        }
    }
}

/// Removes trailing zero digits from a base-10000 digit sequence.
///
/// This function strips unnecessary trailing zeros while preserving at least
/// one digit to represent zero values correctly.
fn strip_trailing_zeros(digits: &mut Vec<i16>) {
    while let Some(&last) = digits.last() {
        if last == 0 && digits.len() > 1 {
            digits.pop();
        } else {
            break;
        }
    }
}

impl std::fmt::Display for PgNumeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PgNumeric::NaN => write!(f, "NaN"),
            PgNumeric::PositiveInfinity => write!(f, "Infinity"),
            PgNumeric::NegativeInfinity => write!(f, "-Infinity"),
            PgNumeric::Value {
                sign,
                weight,
                scale,
                digits,
            } => format_numeric_value(f, sign, *weight, *scale, digits),
        }
    }
}

/// Formats a numeric value for display as a decimal string.
///
/// This function converts a [`PgNumeric::Value`] back into human-readable
/// decimal format, handling the base-10000 to decimal conversion, proper
/// decimal point placement, and scale formatting.
fn format_numeric_value(
    f: &mut std::fmt::Formatter<'_>,
    sign: &Sign,
    weight: i16,
    scale: u16,
    digits: &[i16],
) -> std::fmt::Result {
    // Handle zero case
    if digits.is_empty() {
        return write!(f, "0");
    }

    // Output negative sign if needed
    if matches!(sign, Sign::Negative) {
        write!(f, "-")?;
    }

    if weight < 0 {
        // Number is less than 1, start with "0."
        write!(f, "0")?;
    } else {
        // Output digits before the decimal point
        for d in 0..=weight {
            let base_10000_digit = if (d as usize) < digits.len() {
                digits[d as usize]
            } else {
                0
            };

            // Convert base-10000 digit to 4 decimal digits
            let decimal_digits = format!("{base_10000_digit:04}");

            if d == 0 {
                // For the first digit, suppress leading zeros
                let trimmed = decimal_digits.trim_start_matches('0');
                if trimmed.is_empty() {
                    write!(f, "0")?;
                } else {
                    write!(f, "{trimmed}")?;
                }
            } else {
                // For subsequent digits, always output all 4 digits
                write!(f, "{decimal_digits}")?;
            }
        }
    }

    // Output decimal point and fractional digits if scale > 0
    if scale > 0 {
        write!(f, ".")?;

        let mut remaining_scale = scale as i32;
        let mut d = weight + 1;

        while remaining_scale > 0 {
            let base_10000_digit = if d >= 0 && (d as usize) < digits.len() {
                digits[d as usize]
            } else {
                0
            };

            // Convert base-10000 digit to 4 decimal digits
            let decimal_digits = format!("{base_10000_digit:04}");

            // Output up to remaining_scale digits
            let digits_to_output = std::cmp::min(4, remaining_scale);
            for i in 0..digits_to_output {
                if let Some(ch) = decimal_digits.chars().nth(i as usize) {
                    write!(f, "{ch}")?;
                }
            }

            remaining_scale -= digits_to_output;
            d += 1;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_integer() {
        let result = PgNumeric::from_str("123").unwrap();
        if let PgNumeric::Value {
            sign,
            weight: _,
            scale,
            digits,
        } = result
        {
            assert_eq!(sign, Sign::Positive);
            assert_eq!(scale, 0);
            assert_eq!(digits, vec![123]);
        } else {
            panic!("Invalid PgNumeric value");
        }
    }

    #[test]
    fn parse_negative() {
        let result = PgNumeric::from_str("-456").unwrap();
        if let PgNumeric::Value {
            sign,
            weight: _,
            scale,
            digits,
        } = result
        {
            assert_eq!(sign, Sign::Negative);
            assert_eq!(scale, 0);
            assert_eq!(digits, vec![456]);
        } else {
            panic!("Invalid PgNumeric value");
        }
    }

    #[test]
    fn parse_decimal() {
        let result = PgNumeric::from_str("123.45").unwrap();
        if let PgNumeric::Value {
            sign,
            weight: _,
            scale,
            digits,
        } = result
        {
            assert_eq!(sign, Sign::Positive);
            assert_eq!(scale, 2);
            assert_eq!(digits, vec![123, 4500]);
        } else {
            panic!("Invalid PgNumeric value");
        }
    }

    #[test]
    fn parse_special_values() {
        assert_eq!(PgNumeric::from_str("NaN").unwrap(), PgNumeric::NaN);
        assert_eq!(
            PgNumeric::from_str("Infinity").unwrap(),
            PgNumeric::PositiveInfinity
        );
        assert_eq!(
            PgNumeric::from_str("-Infinity").unwrap(),
            PgNumeric::NegativeInfinity
        );
        assert_eq!(
            PgNumeric::from_str("inf").unwrap(),
            PgNumeric::PositiveInfinity
        );
        assert_eq!(
            PgNumeric::from_str("-inf").unwrap(),
            PgNumeric::NegativeInfinity
        );
    }

    #[test]
    fn parse_scientific_notation() {
        let result = PgNumeric::from_str("1.23e2").unwrap();
        if let PgNumeric::Value {
            sign,
            weight,
            scale,
            digits,
        } = result
        {
            assert_eq!(sign, Sign::Positive);
            assert_eq!(weight, 0);
            assert_eq!(scale, 0);
            assert_eq!(digits, vec![123]);
        } else {
            panic!("Expected Value variant");
        }
    }

    #[test]
    fn parse_errors() {
        assert!(PgNumeric::from_str("").is_err());
        assert!(PgNumeric::from_str("abc").is_err());
        assert!(PgNumeric::from_str("1.2.3").is_err());
        assert!(PgNumeric::from_str("-NaN").is_err()); // NaN cannot have a sign
    }

    #[test]
    fn display_special_values() {
        assert_eq!(format!("{}", PgNumeric::NaN), "NaN");
        assert_eq!(format!("{}", PgNumeric::PositiveInfinity), "Infinity");
        assert_eq!(format!("{}", PgNumeric::NegativeInfinity), "-Infinity");
    }

    #[test]
    fn display_simple_integers() {
        let num = PgNumeric::Value {
            sign: Sign::Positive,
            weight: 0,
            scale: 0,
            digits: vec![123],
        };
        assert_eq!(format!("{num}"), "123");

        let num = PgNumeric::Value {
            sign: Sign::Negative,
            weight: 0,
            scale: 0,
            digits: vec![456],
        };
        assert_eq!(format!("{num}"), "-456");
    }

    #[test]
    fn display_decimals() {
        let num = PgNumeric::Value {
            sign: Sign::Positive,
            weight: 0,
            scale: 2,
            digits: vec![1234, 5000],
        };
        assert_eq!(format!("{num}"), "1234.50");
    }

    #[test]
    fn display_zero() {
        let num = PgNumeric::Value {
            sign: Sign::Positive,
            weight: 0,
            scale: 0,
            digits: vec![],
        };
        assert_eq!(format!("{num}"), "0");
    }

    #[test]
    fn display_large_numbers() {
        let num = PgNumeric::Value {
            sign: Sign::Positive,
            weight: 1,
            scale: 0,
            digits: vec![1234, 5678],
        };
        assert_eq!(format!("{num}"), "12345678");
    }

    #[test]
    fn display_small_decimals() {
        let num = PgNumeric::Value {
            sign: Sign::Positive,
            weight: -1,
            scale: 4,
            digits: vec![1234],
        };
        assert_eq!(format!("{num}"), "0.1234");
    }

    #[test]
    fn leading_zero_suppression() {
        let num = PgNumeric::Value {
            sign: Sign::Positive,
            weight: 0,
            scale: 0,
            digits: vec![123], // First digit has leading zeros when formatted as 4 digits
        };
        assert_eq!(format!("{num}"), "123"); // Should not display as "0123"
    }

    #[test]
    fn trailing_decimal_zeros() {
        let num = PgNumeric::Value {
            sign: Sign::Positive,
            weight: 0,
            scale: 4,
            digits: vec![1200, 0], // Represents 120.0000
        };
        let output = format!("{num}");
        // Should display trailing zeros according to scale
        assert!(output.ends_with("0000"));
    }
}
