//! Fuzz-only entrypoints for internal parsers.

use crate::{error::EtlResult, postgres::codec};

/// Parses a Postgres bytea hex string through the internal parser.
pub fn parse_bytea_hex_string(value: &str) -> EtlResult<Vec<u8>> {
    codec::parse_bytea_hex_string_for_fuzzing(value)
}
