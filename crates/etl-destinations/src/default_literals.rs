//! Generated Postgres string defaults for destination default-rendering tests.

use proptest::{collection::vec, prelude::*};

/// Characters that quoting and escaping rules treat specially in at least one
/// destination dialect.
const SPECIAL_CHARS: &[char] = &[
    '\'', '"', '\\', '\n', '\r', '\t', '?', '`', '$', '\u{1}', '\u{8}', '\u{c}', '\u{b}', '\u{7f}',
    '\u{85}', '\u{2028}', 'é', '😀',
];

/// Postgres text values biased toward characters that need escaping.
///
/// Postgres text cannot contain NUL, so it is never generated.
pub(crate) fn default_string_value() -> impl Strategy<Value = String> {
    let char = prop_oneof![
        3 => prop::sample::select(SPECIAL_CHARS),
        2 => any::<char>().prop_filter("postgres text cannot contain NUL", |ch| *ch != '\0'),
        2 => prop::char::range('a', 'z'),
    ];
    vec(char, 0..24).prop_map(|chars| chars.into_iter().collect())
}

/// Returns `value` as Postgres stores a text column default: a standard
/// single-quoted literal with a cast, as `pg_get_expr` prints it.
pub(crate) fn postgres_text_default(value: &str) -> String {
    format!("'{}'::text", value.replace('\'', "''"))
}
