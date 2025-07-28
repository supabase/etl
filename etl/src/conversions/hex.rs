use crate::bail;
use crate::error::EtlError;
use crate::error::{ErrorKind, EtlResult};

pub fn from_bytea_hex(s: &str) -> EtlResult<Vec<u8>> {
    if s.len() < 2 || &s[..2] != "\\x" {
        bail!(
            ErrorKind::ConversionError,
            "Could not convert from bytea hex string to byte array",
            "The prefix '\\x' is missing"
        );
    }

    let mut result = Vec::with_capacity((s.len() - 2) / 2);
    let s = &s[2..];

    if s.len() % 2 != 0 {
        bail!(
            ErrorKind::ConversionError,
            "Could not convert from bytea hex string to byte array",
            "The number of digits is odd"
        );
    }

    for i in (0..s.len()).step_by(2) {
        let val = u8::from_str_radix(&s[i..i + 2], 16)?;
        result.push(val);
    }

    Ok(result)
}
