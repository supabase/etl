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
    use etl::{schema::PgLsn, test_utils::property::run_property};
    use proptest::{prelude::*, prop_oneof};

    use super::*;

    /// WAL positions biased toward shared LSNs and hex-digit boundaries, where
    /// a wrong digit case or width would reorder tokens.
    fn wal_position() -> impl Strategy<Value = (u64, u64)> {
        let component = prop_oneof![
            4 => any::<u64>(),
            2 => 0u64..=0x20,
            1 => (0u32..64).prop_map(|shift| 1u64 << shift),
            1 => (1u32..64).prop_map(|shift| (1u64 << shift) - 1),
            1 => Just(u64::MAX),
        ];
        (component.clone(), component)
    }

    /// Checks that `token` is exactly what [`OffsetToken::new`] renders for
    /// its decoded position.
    fn assert_canonical(
        input: &str,
        token: &OffsetToken,
    ) -> std::result::Result<(), TestCaseError> {
        prop_assert_eq!(token.to_string(), input);
        let (lsn, ordinal) = token.decode().map_err(|error| {
            TestCaseError::fail(format!("parsed token {input:?} failed decode: {error}"))
        })?;
        prop_assert_eq!(&OffsetToken::new(lsn, ordinal), token);
        Ok(())
    }

    /// Token order, token string byte order (what the materialization query
    /// compares under `collate(seq, 'utf8')`), and WAL tuple order agree.
    #[test]
    fn token_order_matches_wal_order() {
        let strategy = (wal_position(), wal_position(), any::<bool>());
        run_property("offset token order matches wal order", &strategy, |(a, b, share_lsn)| {
            // Equal LSNs make the transaction ordinal decide the order.
            let b = &if *share_lsn { (a.0, b.1) } else { *b };
            let token_a = OffsetToken::new(PgLsn::from(a.0), a.1);
            let token_b = OffsetToken::new(PgLsn::from(b.0), b.1);
            prop_assert_eq!(token_a.cmp(&token_b), a.cmp(b));
            prop_assert_eq!(token_a.as_ref().as_bytes().cmp(token_b.as_ref().as_bytes()), a.cmp(b));

            let parsed: OffsetToken = token_a.to_string().parse().unwrap();
            prop_assert_eq!(&parsed, &token_a);
            let (lsn, ordinal) = parsed.decode().unwrap();
            prop_assert_eq!((u64::from(lsn), ordinal), *a);
            Ok(())
        });
    }

    /// Every accepted string is the canonical rendering of its own position,
    /// so a token read back from Snowflake orders like a locally built one.
    ///
    /// Uppercase hex is excluded: ETL renders every token in lowercase and
    /// Snowflake echoes it back unchanged, so uppercase input does not occur.
    #[test]
    fn only_canonical_tokens_parse() {
        let near_miss = "[0-9a-f+ x_-]{15,17}/[0-9a-f+ x_-]{15,17}";
        let strategy = prop_oneof![
            3 => "[0-9a-f]{16}/[0-9a-f]{16}",
            3 => near_miss,
            1 => "[0-9a-f/]{30,36}",
            1 => "\\PC{0,40}".prop_filter("tokens are always lowercase", |s| {
                !s.bytes().any(|byte| byte.is_ascii_uppercase())
            }),
        ];
        run_property("only canonical offset tokens parse", &strategy, |input| {
            if let Ok(token) = input.parse::<OffsetToken>() {
                assert_canonical(input, &token)?;
            }
            Ok(())
        });
    }

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
