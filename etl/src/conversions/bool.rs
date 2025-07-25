use std::io::ErrorKind;
use thiserror::Error;

use crate::bail;
use crate::error::ETLResult;

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
