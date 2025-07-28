use crate::bail;
use crate::error::EtlResult;
use crate::error::{ErrorKind, EtlError};

pub fn parse_bool(s: &str) -> EtlResult<bool> {
    if s == "t" {
        Ok(true)
    } else if s == "f" {
        Ok(false)
    } else {
        bail!(
            ErrorKind::InvalidData,
            "Invalid boolean value",
            format!("Boolean value must be 't' or 'f' (received: {s})")
        );
    }
}
