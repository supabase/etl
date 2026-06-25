//! Fuzz-only entrypoints for internal parsers.

use crate::{conversions, error::EtlResult};

/// Parses a Postgres bytea hex string through the internal parser.
pub fn parse_bytea_hex_string(value: &str) -> EtlResult<Vec<u8>> {
    conversions::parse_bytea_hex_string_for_fuzzing(value)
}
