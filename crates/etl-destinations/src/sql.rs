//! Shared SQL helpers for destination implementations.

/// Quotes an ANSI-style double-quoted SQL identifier.
///
/// This helper is shared only by destinations whose quoted identifiers escape
/// embedded double quotes by doubling them.
pub(crate) fn quote_double_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}
