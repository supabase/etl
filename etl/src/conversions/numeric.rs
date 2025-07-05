// adapted from the bigdecimal crate
use bigdecimal::{
    BigDecimal, ParseBigDecimalError,
    num_bigint::{BigInt, BigUint, Sign},
};
use byteorder::{BigEndian, ReadBytesExt};
use std::{fmt::Display, io::Cursor, str::FromStr};
use tokio_postgres::types::{FromSql, Type};

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
