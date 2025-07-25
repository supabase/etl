use crate::bail;
use crate::error::ETLResult;
use crate::error::{ETLError, ErrorKind};

pub fn parse_bool(s: &str) -> ETLResult<bool> {
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
