//! Postgres `NUMERIC` value support.

use std::{io::Cursor, str::FromStr};

use byteorder::{BigEndian, ReadBytesExt};
use tokio_postgres::types::{FromSql, IsNull, ToSql, Type};

const POSITIVE_SIGN: u16 = 0x0000;
const NEGATIVE_SIGN: u16 = 0x4000;
const NAN_SIGN: u16 = 0xC000; // NUMERIC_NAN
const POSITIVE_INFINITY_SIGN: u16 = 0xD000; // NUMERIC_PINF
const NEGATIVE_INFINITY_SIGN: u16 = 0xF000; // NUMERIC_NINF
const POSTGRES_NUMERIC_MAX_SCALE: u32 = 16_383;
const POSTGRES_NUMERIC_MIN_WEIGHT: i32 = i16::MIN as i32;
const POSTGRES_NUMERIC_MAX_WEIGHT: i32 = i16::MAX as i32;

/// Sign indicator for Postgres numeric values.
///
/// [`Sign`] represents whether a numeric value is positive or negative,
/// used internally in the Postgres numeric wire format representation.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub enum Sign {
    /// Positive numeric value.
    Positive,
    /// Negative numeric value.
    Negative,
}

/// Error indicating an invalid sign value in Postgres numeric format.
///
/// [`InvalidSign`] wraps the invalid sign value encountered when parsing
/// numeric data from Postgres's wire format.
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

/// Postgres NUMERIC/DECIMAL type with arbitrary precision.
///
/// [`PgNumeric`] represents Postgres's NUMERIC and DECIMAL types, which support
/// arbitrary precision arithmetic. This enum closely matches Postgres's
/// internal wire format and can represent special values like NaN and infinity.
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
        /// For example, if weight=2, the first digit represents multiples of
        /// 10000^2.
        weight: i16,
        /// Number of decimal digits after the decimal point for display
        /// purposes.
        scale: u16,
        /// Actual numeric digits stored in base-10000 format for efficiency.
        digits: Vec<i16>,
    },
}

const ZERO: PgNumeric =
    PgNumeric::Value { sign: Sign::Positive, weight: 0, scale: 0, digits: vec![] };

impl Default for PgNumeric {
    fn default() -> Self {
        ZERO
    }
}

impl FromStr for PgNumeric {
    type Err = ParseNumericError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        // Surrounding whitespace is allowed; `str::trim` uses the same Unicode
        // whitespace predicate the previous char-based skip used. All accepted
        // syntax beyond that is ASCII, so the value is parsed as bytes.
        let trimmed = input.trim();

        if trimmed.is_empty() {
            return Err(ParseNumericError::InvalidSyntax);
        }

        let bytes = trimmed.as_bytes();

        // Handle sign
        let (sign, rest, has_explicit_sign) = match bytes[0] {
            b'+' => (Sign::Positive, &bytes[1..], true),
            b'-' => (Sign::Negative, &bytes[1..], true),
            _ => (Sign::Positive, bytes, false),
        };

        // Check for special values (NaN, infinity)
        if !matches!(rest.first(), Some(b'0'..=b'9' | b'.')) {
            // The sign byte is ASCII, so slicing past it stays on a char
            // boundary.
            let rest = &trimmed[trimmed.len() - rest.len()..];
            return parse_special_value(rest, &sign, has_explicit_sign);
        }

        // Parse regular numeric value
        parse_numeric_value(rest, sign)
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
                    format!("Invalid sign {v:#04x}"),
                )
                .into());
            }
        };

        let scale = rdr.read_u16::<BigEndian>()?;

        let mut digits = Vec::with_capacity(num_digits as usize);
        for _ in 0..num_digits {
            digits.push(rdr.read_i16::<BigEndian>()?);
        }

        Ok(PgNumeric::Value { sign, weight, scale, digits })
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
            PgNumeric::Value { sign: Sign::Positive, weight, scale, digits } => {
                (POSITIVE_SIGN, weight, scale, digits)
            }
            PgNumeric::Value { sign: Sign::Negative, weight, scale, digits } => {
                (NEGATIVE_SIGN, weight, scale, digits)
            }
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
/// This function handles Postgres's special numeric values when they appear
/// in string format. NaN cannot have a negative sign, while infinity values
/// respect the sign parameter. The candidates are ASCII, so case-insensitive
/// ASCII comparison accepts and rejects the same inputs the previous
/// Unicode-lowercasing comparison did, without allocating.
fn parse_special_value(
    rest: &str,
    sign: &Sign,
    has_explicit_sign: bool,
) -> Result<PgNumeric, ParseNumericError> {
    let rest = rest.trim_end();

    if rest.eq_ignore_ascii_case("nan") {
        // NaN must not have a sign.
        if has_explicit_sign {
            return Err(ParseNumericError::InvalidSyntax);
        }
        Ok(PgNumeric::NaN)
    } else if rest.eq_ignore_ascii_case("infinity") || rest.eq_ignore_ascii_case("inf") {
        match sign {
            Sign::Positive => Ok(PgNumeric::PositiveInfinity),
            Sign::Negative => Ok(PgNumeric::NegativeInfinity),
        }
    } else {
        Err(ParseNumericError::InvalidSyntax)
    }
}

/// Parses a regular numeric value from trimmed byte input.
///
/// This function processes decimal digits, decimal points, underscores (digit
/// separators), and scientific notation to construct a numeric value. It
/// handles both integer and fractional parts with arbitrary precision. All
/// accepted syntax is ASCII, so a non-ASCII byte simply fails to match and
/// rejects the input, exactly as the previous char-based parser did.
fn parse_numeric_value(bytes: &[u8], sign: Sign) -> Result<PgNumeric, ParseNumericError> {
    let mut decimal_digits = Vec::with_capacity(bytes.len());
    let mut have_decimal_point = false;
    let mut dweight = -1i32; // Decimal weight (number of digits before decimal point - 1)
    let mut dscale = 0u32; // Number of digits after decimal point
    let mut pos = 0;

    // Check for initial decimal point
    if bytes.first() == Some(&b'.') {
        have_decimal_point = true;
        pos += 1;
    }

    // Must have at least one digit
    if !matches!(bytes.get(pos), Some(b'0'..=b'9')) {
        return Err(ParseNumericError::InvalidSyntax);
    }

    // Parse digits and decimal point
    while let Some(&byte) = bytes.get(pos) {
        match byte {
            b'0'..=b'9' => {
                pos += 1;
                decimal_digits.push(byte - b'0');
                if !have_decimal_point {
                    dweight += 1;
                } else {
                    dscale += 1;
                }
            }
            b'.' => {
                if have_decimal_point {
                    return Err(ParseNumericError::InvalidSyntax);
                }
                have_decimal_point = true;
                pos += 1;
                // Decimal point must not be followed by underscore
                if bytes.get(pos) == Some(&b'_') {
                    return Err(ParseNumericError::InvalidSyntax);
                }
            }
            b'_' => {
                pos += 1;
                // Underscore must be followed by more digits
                if !matches!(bytes.get(pos), Some(b'0'..=b'9')) {
                    return Err(ParseNumericError::InvalidSyntax);
                }
            }
            _ => break,
        }
    }

    // Handle scientific notation
    if matches!(bytes.get(pos), Some(b'e' | b'E')) {
        pos += 1;
        let mut exponent = 0i64;
        let mut exp_negative = false;

        // Handle exponent sign
        match bytes.get(pos) {
            Some(b'+') => {
                pos += 1;
            }
            Some(b'-') => {
                exp_negative = true;
                pos += 1;
            }
            _ => {}
        }

        // Parse exponent digits
        if !matches!(bytes.get(pos), Some(b'0'..=b'9')) {
            return Err(ParseNumericError::InvalidSyntax);
        }

        while let Some(&byte) = bytes.get(pos) {
            match byte {
                b'0'..=b'9' => {
                    pos += 1;
                    exponent = exponent * 10 + i64::from(byte - b'0');
                    // This is an arithmetic overflow guard, not the Postgres
                    // numeric range check. Weight and scale bounds are
                    // enforced below after the exponent is applied to the
                    // parsed decimal shape.
                    if exponent > i32::MAX as i64 / 2 {
                        return Err(ParseNumericError::ValueOutOfRange);
                    }
                }
                b'_' => {
                    pos += 1;
                    if !matches!(bytes.get(pos), Some(b'0'..=b'9')) {
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
        dscale = if (dscale as i64 - exponent) < 0 { 0 } else { (dscale as i64 - exponent) as u32 };
    }

    // The caller trims surrounding whitespace, so any remaining byte is junk.
    if pos != bytes.len() {
        return Err(ParseNumericError::InvalidSyntax);
    }

    if dscale > POSTGRES_NUMERIC_MAX_SCALE {
        return Err(ParseNumericError::ValueOutOfRange);
    }

    // Convert to base-10000 representation
    convert_to_base_10000(&decimal_digits, dweight, dscale, sign)
}

/// Converts decimal digits to Postgres's base-10000 internal format.
///
/// This function transforms a sequence of decimal digits into Postgres's
/// efficient base-10000 representation, where each internal digit represents
/// up to 10000 in decimal. This format balances storage efficiency with
/// calculation performance.
fn convert_to_base_10000(
    decimal_digits: &[u8],
    dweight: i32,
    dscale: u32,
    sign: Sign,
) -> Result<PgNumeric, ParseNumericError> {
    let scale = u16::try_from(dscale).map_err(|_| ParseNumericError::ValueOutOfRange)?;

    if decimal_digits.is_empty() {
        return Ok(PgNumeric::Value { sign: Sign::Positive, weight: 0, scale, digits: vec![] });
    }

    // Calculate weight in base-10000 terms
    // Each base-10000 digit represents 4 decimal digits
    let weight = if dweight >= 0 { (dweight + 4) / 4 - 1 } else { -((-dweight - 1) / 4 + 1) };

    // Calculate offset for proper alignment
    let offset = (weight + 1) * 4 - (dweight + 1);
    let Some(first_nonzero_decimal_index) = decimal_digits.iter().position(|&digit| digit != 0)
    else {
        return Ok(PgNumeric::Value { sign: Sign::Positive, weight: 0, scale, digits: vec![] });
    };
    let last_nonzero_decimal_index = decimal_digits
        .iter()
        .rposition(|&digit| digit != 0)
        .expect("nonzero decimal digit should have a last position");

    let first_nonzero_group = (offset + first_nonzero_decimal_index as i32) / 4;
    let last_nonzero_group = (offset + last_nonzero_decimal_index as i32) / 4;
    let final_weight = weight - first_nonzero_group;
    if !(POSTGRES_NUMERIC_MIN_WEIGHT..=POSTGRES_NUMERIC_MAX_WEIGHT).contains(&final_weight) {
        return Err(ParseNumericError::ValueOutOfRange);
    }

    // Convert groups of 4 decimal digits to base-10000 digits, reading each
    // decimal digit at its aligned position and treating positions outside the
    // input as zero padding. Leading and trailing zero groups are skipped up
    // front, so obviously out-of-range exponents are rejected before allocating
    // a vector sized by the exponent.
    let retained_groups = last_nonzero_group - first_nonzero_group + 1;
    let mut base_10000_digits = Vec::with_capacity(retained_groups as usize);
    for group in first_nonzero_group..=last_nonzero_group {
        let mut digit = 0i16;
        for position in group * 4..group * 4 + 4 {
            let decimal_digit = usize::try_from(position - offset)
                .ok()
                .and_then(|index| decimal_digits.get(index).copied())
                .unwrap_or(0);
            digit = digit * 10 + decimal_digit as i16;
        }
        base_10000_digits.push(digit);
    }

    Ok(PgNumeric::Value { sign, weight: final_weight as i16, scale, digits: base_10000_digits })
}

impl std::fmt::Display for PgNumeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PgNumeric::NaN => write!(f, "NaN"),
            PgNumeric::PositiveInfinity => write!(f, "Infinity"),
            PgNumeric::NegativeInfinity => write!(f, "-Infinity"),
            PgNumeric::Value { sign, weight, scale, digits } => {
                format_numeric_value(f, sign, *weight, *scale, digits)
            }
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
            let base_10000_digit = if (d as usize) < digits.len() { digits[d as usize] } else { 0 };

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
            let base_10000_digit =
                if d >= 0 && (d as usize) < digits.len() { digits[d as usize] } else { 0 };

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
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn parse_simple_integer() {
        let result = PgNumeric::from_str("123").unwrap();
        if let PgNumeric::Value { sign, weight: _, scale, digits } = result {
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
        if let PgNumeric::Value { sign, weight: _, scale, digits } = result {
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
        if let PgNumeric::Value { sign, weight: _, scale, digits } = result {
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
        assert_eq!(PgNumeric::from_str("NaN   ").unwrap(), PgNumeric::NaN);
        assert_eq!(PgNumeric::from_str("+NaN").unwrap_err(), ParseNumericError::InvalidSyntax);
        assert_eq!(PgNumeric::from_str("-NaN").unwrap_err(), ParseNumericError::InvalidSyntax);
        assert_eq!(PgNumeric::from_str("Infinity").unwrap(), PgNumeric::PositiveInfinity);
        assert_eq!(PgNumeric::from_str("+Infinity   ").unwrap(), PgNumeric::PositiveInfinity);
        assert_eq!(PgNumeric::from_str("-Infinity").unwrap(), PgNumeric::NegativeInfinity);
        assert_eq!(PgNumeric::from_str("inf").unwrap(), PgNumeric::PositiveInfinity);
        assert_eq!(PgNumeric::from_str("-inf").unwrap(), PgNumeric::NegativeInfinity);
    }

    #[test]
    fn parse_postgres_numeric_weight_boundaries() {
        let max_weight = PgNumeric::from_str("1e131071").unwrap();
        if let PgNumeric::Value { sign, weight, scale, ref digits } = max_weight {
            assert_eq!(sign, Sign::Positive);
            assert_eq!(weight, i16::MAX);
            assert_eq!(scale, 0);
            assert_eq!(digits.as_slice(), &[1000]);
        } else {
            panic!("Expected Value variant");
        }

        let min_scale_weight = PgNumeric::from_str("1e-16383").unwrap();
        if let PgNumeric::Value { sign, weight, scale, ref digits } = min_scale_weight {
            assert_eq!(sign, Sign::Positive);
            assert_eq!(weight, -4096);
            assert_eq!(scale, POSTGRES_NUMERIC_MAX_SCALE as u16);
            assert_eq!(digits.as_slice(), &[10]);
        } else {
            panic!("Expected Value variant");
        }

        assert_eq!(
            PgNumeric::from_str("1e131072").unwrap_err(),
            ParseNumericError::ValueOutOfRange
        );
        assert_eq!(
            PgNumeric::from_str("1e-16384").unwrap_err(),
            ParseNumericError::ValueOutOfRange
        );
        assert_eq!(
            PgNumeric::from_str("1e1000000000").unwrap_err(),
            ParseNumericError::ValueOutOfRange
        );
        assert_eq!(
            PgNumeric::from_str("1e-1000000000").unwrap_err(),
            ParseNumericError::ValueOutOfRange
        );
    }

    #[test]
    fn parse_postgres_numeric_max_shape_roundtrips_sql() {
        let mut value = String::with_capacity(
            POSTGRES_NUMERIC_MAX_SCALE as usize + POSTGRES_NUMERIC_MAX_WEIGHT as usize * 4 + 5,
        );
        value.push_str(&"9".repeat((POSTGRES_NUMERIC_MAX_WEIGHT as usize + 1) * 4));
        value.push('.');
        value.push_str(&"9".repeat(POSTGRES_NUMERIC_MAX_SCALE as usize));

        let numeric = PgNumeric::from_str(&value).unwrap();
        if let PgNumeric::Value { ref sign, weight, scale, ref digits } = numeric {
            assert_eq!(*sign, Sign::Positive);
            assert_eq!(weight, i16::MAX);
            assert_eq!(scale, POSTGRES_NUMERIC_MAX_SCALE as u16);
            assert_eq!(digits.len(), 36_864);
            assert_eq!(digits.first(), Some(&9999));
            assert_eq!(digits.last(), Some(&9990));
        } else {
            panic!("Expected Value variant");
        }

        let mut buf = BytesMut::new();
        ToSql::to_sql(&numeric, &Type::NUMERIC, &mut buf).unwrap();
        let round = PgNumeric::from_sql(&Type::NUMERIC, &buf).unwrap();
        assert_eq!(numeric, round);
    }

    #[test]
    fn parse_scientific_notation() {
        let result = PgNumeric::from_str("1.23e2").unwrap();
        if let PgNumeric::Value { sign, weight, scale, digits } = result {
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
        let num = PgNumeric::Value { sign: Sign::Positive, weight: 0, scale: 0, digits: vec![123] };
        assert_eq!(format!("{num}"), "123");

        let num = PgNumeric::Value { sign: Sign::Negative, weight: 0, scale: 0, digits: vec![456] };
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
        let num = PgNumeric::Value { sign: Sign::Positive, weight: 0, scale: 0, digits: vec![] };
        assert_eq!(format!("{num}"), "0");
    }

    #[test]
    fn zero_canonicalization_basic() {
        for s in ["0", "0.0", "000", "000.000"] {
            let num = PgNumeric::from_str(s).unwrap();
            assert_eq!(num.to_string(), "0");

            if let PgNumeric::Value { sign, weight, scale: _, digits } = num {
                assert_eq!(sign, Sign::Positive);
                assert_eq!(weight, 0);
                assert!(digits.is_empty());
            } else {
                panic!("Expected Value variant");
            }
        }
    }

    #[test]
    fn zero_canonicalization_negative_zero() {
        for s in ["-0", "-0.00"] {
            let num = PgNumeric::from_str(s).unwrap();
            assert_eq!(num.to_string(), "0");

            if let PgNumeric::Value { sign, weight, scale: _, digits } = num {
                // Normalize to positive zero
                assert_eq!(sign, Sign::Positive);
                assert_eq!(weight, 0);
                assert!(digits.is_empty());
            } else {
                panic!("Expected Value variant");
            }
        }
    }

    #[test]
    fn zero_roundtrip_sql() {
        for s in ["0", "0.000"] {
            let num = PgNumeric::from_str(s).unwrap();
            let mut buf = BytesMut::new();
            ToSql::to_sql(&num, &Type::NUMERIC, &mut buf).unwrap();
            let round = PgNumeric::from_sql(&Type::NUMERIC, &buf).unwrap();
            // Internal representation should be canonical-zero with same scale
            assert_eq!(num, round);
            if let PgNumeric::Value { sign, weight, digits, .. } = round {
                assert_eq!(sign, Sign::Positive);
                assert_eq!(weight, 0);
                assert!(digits.is_empty());
            } else {
                panic!("Expected Value variant");
            }
        }
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
        let num =
            PgNumeric::Value { sign: Sign::Positive, weight: -1, scale: 4, digits: vec![1234] };
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

    #[test]
    fn weight_ignores_trailing_fraction_groups() {
        // 0.0012000 → groups: [12, 0], weight must stay at -1 after stripping
        let num = PgNumeric::from_str("0.0012000").unwrap();
        assert_eq!(num.to_string(), "0.0012000");

        if let PgNumeric::Value { sign, weight, scale, ref digits } = num {
            assert_eq!(sign, Sign::Positive);
            assert_eq!(weight, -1, "weight should remain -1");
            assert_eq!(scale, 7, "scale preserved for display");
            assert_eq!(digits.as_slice(), &[12], "trailing base-10000 zero group stripped");
        } else {
            panic!("Expected Value variant");
        }
    }

    #[test]
    fn weight_and_groups_boundary_cases() {
        // 9999.9999 is exactly two full groups: [9999, 9999], weight 0
        let num_1 = PgNumeric::from_str("9999.9999").unwrap();
        assert_eq!(num_1.to_string(), "9999.9999");

        if let PgNumeric::Value { sign, weight, scale, ref digits } = num_1 {
            assert_eq!(sign, Sign::Positive);
            assert_eq!(weight, 0);
            assert_eq!(scale, 4);
            assert_eq!(digits.as_slice(), &[9999, 9999]);
        } else {
            panic!("Expected Value variant");
        }

        // 10000.0001 crosses the 10^4 boundary
        let num_2 = PgNumeric::from_str("10000.0001").unwrap();
        assert_eq!(num_2.to_string(), "10000.0001");

        if let PgNumeric::Value { sign, weight, scale, ref digits } = num_2 {
            assert_eq!(sign, Sign::Positive);
            assert_eq!(weight, 1);
            assert_eq!(scale, 4);
            // Two integer groups [1, 0] and one fractional group [1]
            assert_eq!(digits.as_slice(), &[1, 0, 1]);
        } else {
            panic!("Expected Value variant");
        }
    }

    #[test]
    fn ignores_input_leading_zeros() {
        let num = PgNumeric::from_str("0000120.00").unwrap();
        assert_eq!(num.to_string(), "120.00");

        if let PgNumeric::Value { sign, weight, scale, ref digits } = num {
            assert_eq!(sign, Sign::Positive);
            assert_eq!(weight, 0);
            assert_eq!(scale, 2);
            assert_eq!(digits.as_slice(), &[120]);
        } else {
            panic!("Expected Value variant");
        }
    }

    #[test]
    fn roundtrip_stability() {
        let cases = ["120.00", "1.2000", "0.0120", "9999.9999", "10000.0001", "-120.00", "1200000"];

        for case in &cases {
            let parsed = PgNumeric::from_str(case).unwrap();
            let printed = parsed.to_string();
            let reparsed = PgNumeric::from_str(&printed).unwrap();

            // String should be stable across two parses
            assert_eq!(printed, reparsed.to_string(), "unstable print for {case}");

            // Value representation should be equal across parse/print/parse
            assert_eq!(parsed, reparsed, "unstable internal value for {case}");
        }
    }

    #[test]
    fn large_integer_weight() {
        // 1,200,000 = 120*10000 + 0 → digits [120, 0] before strip trailing zero
        // We expect trailing zero group to be stripped, weight stays 1.
        let num = PgNumeric::from_str("1200000").unwrap();
        assert_eq!(num.to_string(), "1200000");

        if let PgNumeric::Value { sign, weight, scale, ref digits } = num {
            assert_eq!(sign, Sign::Positive);
            assert_eq!(weight, 1);
            assert_eq!(scale, 0);
            assert_eq!(digits.as_slice(), &[120]);
        } else {
            panic!("Expected Value variant");
        }
    }

    #[test]
    fn tosql_fromsql_special_values() {
        let cases = [PgNumeric::NaN, PgNumeric::PositiveInfinity, PgNumeric::NegativeInfinity];

        for case in cases {
            let mut buf = BytesMut::new();
            ToSql::to_sql(&case, &Type::NUMERIC, &mut buf).unwrap();
            let round = PgNumeric::from_sql(&Type::NUMERIC, &buf).unwrap();
            assert_eq!(format!("{case}"), format!("{}", round));
            assert_eq!(case, round);
        }
    }
}
