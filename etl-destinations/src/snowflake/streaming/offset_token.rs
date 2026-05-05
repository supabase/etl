use std::{fmt, str::FromStr};

use etl::types::PgLsn;

use crate::snowflake::{Error, Result};

/// An offset token encoding a WAL position as a hex string.
///
/// Format: `{commit_lsn:016x}/{tx_ordinal:016x}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetToken(String);

impl OffsetToken {
    /// The zero offset token, used for initial copy rows before any WAL
    /// position.
    pub fn zero() -> Self {
        Self("0000000000000000/0000000000000000".into())
    }

    /// Encode a WAL position as an offset token.
    pub fn new(commit_lsn: PgLsn, tx_ordinal: u64) -> Self {
        Self(format!("{:016x}/{:016x}", u64::from(commit_lsn), tx_ordinal))
    }

    /// Decode the token back to `(commit_lsn, tx_ordinal)`.
    pub fn decode(&self) -> Result<(PgLsn, u64)> {
        let token = self.0.as_str();
        let (lsn_hex, ord_hex) = token
            .split_once('/')
            .ok_or_else(|| Error::Channel(format!("invalid offset token format: {token}")))?;

        let lsn = u64::from_str_radix(lsn_hex, 16)
            .map_err(|e| Error::Channel(format!("invalid LSN hex in offset token: {e}")))?;

        let ord = u64::from_str_radix(ord_hex, 16)
            .map_err(|e| Error::Channel(format!("invalid ordinal hex in offset token: {e}")))?;

        Ok((PgLsn::from(lsn), ord))
    }
}

impl fmt::Display for OffsetToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for OffsetToken {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl FromStr for OffsetToken {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let token = OffsetToken(s.to_owned());
        token.decode()?;
        Ok(token)
    }
}
