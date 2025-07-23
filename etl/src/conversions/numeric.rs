// adapted from the bigdecimal crate
use bigdecimal::{
    BigDecimal, ParseBigDecimalError,
    num_bigint::{BigInt, BigUint, Sign},
};
use byteorder::{BigEndian, ReadBytesExt};
use std::{fmt::Display, io::Cursor, str::FromStr};
use tokio_postgres::types::{FromSql, IsNull, ToSql, Type};

/// A rust variant of the Postgres Numeric type. The full spectrum of Postgres'
/// Numeric value range is supported.
///
/// Represented as an Optional BigDecimal. None for 'NaN', Some(bigdecimal) for
/// all other values.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub enum PgNumeric {
    NaN,
    PositiveInf,
    NegativeInf,
    Value(BigDecimal),
}

impl FromStr for PgNumeric {
    type Err = ParseBigDecimalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match BigDecimal::from_str(s) {
            Ok(n) => Ok(PgNumeric::Value(n)),
            Err(e) => {
                if s.to_lowercase() == "infinity" {
                    Ok(PgNumeric::PositiveInf)
                } else if s.to_lowercase() == "-infinity" {
                    Ok(PgNumeric::NegativeInf)
                } else if s.to_lowercase() == "nan" {
                    Ok(PgNumeric::NaN)
                } else {
                    Err(e)
                }
            }
        }
    }
}

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(
        _: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Sync + Send>> {
        let mut rdr = Cursor::new(raw);

        let n_digits = rdr.read_u16::<BigEndian>()?;
        let weight = rdr.read_i16::<BigEndian>()?;
        let sign = match rdr.read_u16::<BigEndian>()? {
            0x4000 => Sign::Minus,
            0x0000 => Sign::Plus,
            0xC000 => return Ok(PgNumeric::NaN),
            0xD000 => return Ok(PgNumeric::PositiveInf),
            0xF000 => return Ok(PgNumeric::NegativeInf),
            v => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid sign {v:#04x}"),
                )
                .into());
            }
        };
        let scale = rdr.read_u16::<BigEndian>()?;

        let mut biguint = BigUint::from(0u32);
        for n in (0..n_digits).rev() {
            let digit = rdr.read_u16::<BigEndian>()?;
            biguint += BigUint::from(digit) * BigUint::from(10_000u32).pow(n as u32);
        }

        // First digit in unsigned now has factor 10_000^(digits.len() - 1),
        // but should have 10_000^weight
        //
        // Credits: this logic has been copied from rust Diesel's related code
        // that provides the same translation from Postgres numeric into their
        // related rust type.
        let correction_exp = 4 * (i64::from(weight) - i64::from(n_digits) + 1);
        let res = BigDecimal::new(BigInt::from_biguint(sign, biguint), -correction_exp)
            .with_scale(i64::from(scale));

        Ok(PgNumeric::Value(res))
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
        match self {
            PgNumeric::NaN => {
                // Write header for NaN
                out.extend_from_slice(&0u16.to_be_bytes()); // n_digits = 0
                out.extend_from_slice(&0i16.to_be_bytes()); // weight = 0
                out.extend_from_slice(&0xC000u16.to_be_bytes()); // sign = NaN
                out.extend_from_slice(&0u16.to_be_bytes()); // scale = 0
                Ok(IsNull::No)
            }
            PgNumeric::PositiveInf => {
                // Write header for positive infinity
                out.extend_from_slice(&0u16.to_be_bytes()); // n_digits = 0
                out.extend_from_slice(&0i16.to_be_bytes()); // weight = 0
                out.extend_from_slice(&0xD000u16.to_be_bytes()); // sign = PositiveInf
                out.extend_from_slice(&0u16.to_be_bytes()); // scale = 0
                Ok(IsNull::No)
            }
            PgNumeric::NegativeInf => {
                // Write header for negative infinity
                out.extend_from_slice(&0u16.to_be_bytes()); // n_digits = 0
                out.extend_from_slice(&0i16.to_be_bytes()); // weight = 0
                out.extend_from_slice(&0xF000u16.to_be_bytes()); // sign = NegativeInf
                out.extend_from_slice(&0u16.to_be_bytes()); // scale = 0
                Ok(IsNull::No)
            }
            PgNumeric::Value(decimal) => {
                if *decimal == BigDecimal::from(0) {
                    // Special case for zero
                    out.extend_from_slice(&0u16.to_be_bytes()); // n_digits = 0
                    out.extend_from_slice(&0i16.to_be_bytes()); // weight = 0
                    out.extend_from_slice(&0x0000u16.to_be_bytes()); // sign = Plus
                    out.extend_from_slice(&0u16.to_be_bytes()); // scale = 0
                    return Ok(IsNull::No);
                }

                let (bigint, scale) = decimal.as_bigint_and_exponent();
                let sign_flag = if bigint.sign() == Sign::Minus {
                    0x4000u16
                } else {
                    0x0000u16
                };

                // Convert to absolute value and get digits
                let abs_bigint = bigint.magnitude();

                // Convert to string to extract digits more easily
                let decimal_str = abs_bigint.to_string();

                // Group digits into base-10000 chunks (4 decimal digits per postgres digit)
                let mut digits = Vec::new();
                let mut remaining = decimal_str.as_str();

                // Pad with leading zeros if necessary to make length divisible by 4
                let padding = (4 - (decimal_str.len() % 4)) % 4;
                let padded_str = format!("{:0width$}{}", "", remaining, width = padding);
                remaining = &padded_str;

                while remaining.len() >= 4 {
                    let chunk = &remaining[..4];
                    let digit: u16 = chunk.parse().unwrap_or(0);
                    digits.push(digit);
                    remaining = &remaining[4..];
                }

                // Remove leading zeros
                while digits.first() == Some(&0) && digits.len() > 1 {
                    digits.remove(0);
                }

                let n_digits = digits.len() as u16;

                // Calculate weight (position of most significant digit)
                let weight = if scale >= 0 {
                    ((decimal_str.len() as i64 + scale - 1) / 4) as i16
                } else {
                    ((decimal_str.len() as i64 - 1) / 4 - (-scale - 1) / 4) as i16
                };

                // Write the numeric structure
                out.extend_from_slice(&n_digits.to_be_bytes());
                out.extend_from_slice(&weight.to_be_bytes());
                out.extend_from_slice(&sign_flag.to_be_bytes());
                out.extend_from_slice(&(scale.unsigned_abs() as u16).to_be_bytes());

                // Write the digits
                for digit in digits {
                    out.extend_from_slice(&digit.to_be_bytes());
                }

                Ok(IsNull::No)
            }
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }

    tokio_postgres::types::to_sql_checked!();
}

impl Display for PgNumeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PgNumeric::NaN => write!(f, "NaN"),
            PgNumeric::PositiveInf => write!(f, "Infinity"),
            PgNumeric::NegativeInf => write!(f, "-Infinity"),
            PgNumeric::Value(n) => write!(f, "{n}"),
        }
    }
}

impl Default for PgNumeric {
    fn default() -> Self {
        PgNumeric::Value(BigDecimal::default())
    }
}
