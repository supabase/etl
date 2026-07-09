use std::{fmt, str::FromStr};

use etl::schema::PgLsn;

use crate::snowflake::{Error, Result};

/// An offset token encoding a WAL position as a hex string.
///
/// Format: `{commit_lsn:016x}/{tx_ordinal:016x}`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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

impl Default for OffsetToken {
    fn default() -> Self {
        Self::zero()
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
        if !is_canonical_offset_token(s) {
            return Err(Error::Channel(format!("invalid offset token format: {s}")));
        }

        let token = OffsetToken(s.to_owned());
        token.decode()?;
        Ok(token)
    }
}

fn is_canonical_offset_token(s: &str) -> bool {
    let Some((lsn_hex, ordinal_hex)) = s.split_once('/') else {
        return false;
    };

    lsn_hex.len() == 16
        && ordinal_hex.len() == 16
        && lsn_hex.bytes().all(|byte| byte.is_ascii_hexdigit())
        && ordinal_hex.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use etl::schema::PgLsn;

    use super::*;

    #[test]
    fn orders_by_lsn_then_transaction_ordinal() {
        let low_lsn_high_ordinal = OffsetToken::new(PgLsn::from(10), 99);
        let high_lsn_low_ordinal = OffsetToken::new(PgLsn::from(11), 0);
        let same_lsn_low_ordinal = OffsetToken::new(PgLsn::from(11), 1);
        let same_lsn_high_ordinal = OffsetToken::new(PgLsn::from(11), 2);

        assert!(high_lsn_low_ordinal > low_lsn_high_ordinal);
        assert!(same_lsn_high_ordinal > same_lsn_low_ordinal);
        assert!(same_lsn_high_ordinal >= same_lsn_low_ordinal);
        assert!(same_lsn_low_ordinal < same_lsn_high_ordinal);
    }
}
