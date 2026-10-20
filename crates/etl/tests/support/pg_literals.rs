//! Strategies for Postgres input literals shared by the value properties.
//!
//! Each strategy produces text that Postgres accepts when cast from `text` to
//! the named type, so properties can insert any column type through one
//! `$n::text::<type>` parameter shape and let Postgres render the canonical
//! output that replication and COPY carry.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    net::{Ipv4Addr, Ipv6Addr},
    ops::RangeInclusive,
};

use chrono::NaiveTime;
use etl::test_utils::property::{any_f32, any_f64, pg_text, pg_time};
use proptest::{option, prelude::*, sample::select};
use uuid::Uuid;

/// Pieces that stress COPY and array escaping when concatenated: delimiters,
/// escapes, quotes, braces, the COPY `\N` marker, NULL spellings, control
/// characters, and characters outside the Basic Multilingual Plane.
const ESCAPE_HEAVY_PIECES: &[&str] = &[
    "\t", "\n", "\r", "\\", "\\N", "N", "NULL", "null", "\"", "'", "{", "}", ",", ";", " ",
    "\u{1}", "\u{8}", "\u{b}", "\u{c}", "\u{1b}", "\u{7f}", "é", "😀", "\u{feff}", "a",
];

/// Characters that need escaping in JSON strings or exercise multi-byte and
/// surrogate-pair encodings.
const JSON_SPECIAL_CHARS: &[char] = &[
    '"', '\\', '/', '\n', '\t', '\r', '\u{8}', '\u{c}', '\u{1}', '\u{1f}', '\u{7f}', 'é', '😀',
    '\u{feff}', '\0',
];

/// Whitespace rendered between JSON tokens: none, a space, or a mix that
/// needs COPY escaping.
const JSON_WHITESPACE: &[&str] = &["", " ", "\n\t "];

/// Maximum container nesting of the random tree under a [`json_document`]
/// chain.
pub(crate) const JSON_TREE_MAX_DEPTH: usize = 3;

/// Text that needs escaping in COPY output and quoting in array output.
pub(crate) fn escape_heavy_text() -> impl Strategy<Value = String> {
    proptest::collection::vec(select(ESCAPE_HEAVY_PIECES), 0..=8).prop_map(|pieces| pieces.concat())
}

/// Text values mixing arbitrary Postgres text with escape-heavy text.
pub(crate) fn text_literal() -> impl Strategy<Value = String> {
    prop_oneof![pg_text(), escape_heavy_text()]
}

/// Text values that additionally include the NULL spellings and the empty
/// string on their own, which array output must quote to keep them distinct
/// from NULL elements.
pub(crate) fn varchar_literal() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => text_literal(),
        1 => select(&["NULL", "null", "Null", "", " ", "\"\""][..]).prop_map(str::to_owned),
    ]
}

/// `bytea` hex input literals.
pub(crate) fn bytea_literal() -> impl Strategy<Value = String> {
    proptest::collection::vec(any::<u8>(), 0..=16).prop_map(|bytes| {
        let mut literal = String::from("\\x");
        for byte in bytes {
            write!(literal, "{byte:02x}").unwrap();
        }

        literal
    })
}

/// `bool` input literals.
pub(crate) fn bool_literal() -> impl Strategy<Value = String> {
    any::<bool>().prop_map(|value| value.to_string())
}

/// `int2` input literals over the full range.
pub(crate) fn int2_literal() -> impl Strategy<Value = String> {
    any::<i16>().prop_map(|value| value.to_string())
}

/// `int4` input literals over the full range.
pub(crate) fn int4_literal() -> impl Strategy<Value = String> {
    any::<i32>().prop_map(|value| value.to_string())
}

/// `int8` input literals over the full range.
pub(crate) fn int8_literal() -> impl Strategy<Value = String> {
    any::<i64>().prop_map(|value| value.to_string())
}

/// Formats a float as a Postgres input literal, spelling the specials the way
/// `float4in`/`float8in` accept them.
fn float_literal(value: f64, finite: String) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "Infinity".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_owned()
    } else {
        finite
    }
}

/// `float4` input literals over every bit pattern.
///
/// Finite values use the shortest `f32` scientific form, which `float4in`
/// reads back to the same bits.
pub(crate) fn float4_literal() -> impl Strategy<Value = String> {
    any_f32().prop_map(|value| float_literal(f64::from(value), format!("{value:e}")))
}

/// `float8` input literals over every bit pattern.
pub(crate) fn float8_literal() -> impl Strategy<Value = String> {
    any_f64().prop_map(|value| float_literal(value, format!("{value:e}")))
}

/// Decimal literal strings covering integers, fractions, scientific notation,
/// and the numeric specials.
///
/// Exponents are tiered: most cases stay small so typical magnitudes dominate,
/// while the two boundary tiers reach the extremes the codec and Postgres
/// share, `1e131071` on the weight side and `1e-16383` on the scale side. With
/// up to 45 integer and 45 fractional digits, exponents in `[-16_338, 131_027]`
/// always stay inside Postgres's accepted range (verified empirically: one step
/// past either bound overflows the numeric format), so Postgres stays a render
/// oracle and never rejects a generated literal.
pub(crate) fn numeric_literal() -> impl Strategy<Value = String> {
    let digits = |max: usize| proptest::collection::vec(0u8..=9, 1..=max);

    let exponent = prop_oneof![
        8 => -60i32..=60,
        1 => -16_338i32..=-16_200,
        1 => 130_900i32..=131_027,
    ];

    let finite = (any::<bool>(), digits(45), option::of(digits(45)), option::of(exponent))
        .prop_map(|(negative, int_digits, frac_digits, exponent)| {
            let mut literal = String::new();
            if negative {
                literal.push('-');
            }
            for digit in int_digits {
                literal.push((b'0' + digit) as char);
            }
            if let Some(frac_digits) = frac_digits {
                literal.push('.');
                for digit in frac_digits {
                    literal.push((b'0' + digit) as char);
                }
            }
            if let Some(exponent) = exponent {
                literal.push('e');
                literal.push_str(&exponent.to_string());
            }

            literal
        });

    prop_oneof![
        45 => finite,
        1 => Just("NaN".to_owned()),
        1 => Just("Infinity".to_owned()),
        1 => Just("-Infinity".to_owned()),
    ]
}

/// Calendar parts of a finite date: year, BC flag, month, and day.
///
/// BC years stop at 4712 because Postgres dates begin in November 4714 BC
/// (proleptic Gregorian) and AD years stop inside chrono's calendar range,
/// which bounds what the codec can represent. Days stop at 28 so every month
/// is valid.
fn date_parts() -> impl Strategy<Value = (i32, bool, u32, u32)> {
    let year = prop_oneof![
        2 => (1i32..=4712).prop_map(|year| (year, true)),
        6 => (1i32..=9999).prop_map(|year| (year, false)),
        1 => (10_000i32..=262_000).prop_map(|year| (year, false)),
    ];

    (year, 1u32..=12, 1u32..=28).prop_map(|((year, bc), month, day)| (year, bc, month, day))
}

/// Returns the ` BC` era suffix Postgres uses for BC dates, or nothing.
fn era_suffix(bc: bool) -> &'static str {
    if bc { " BC" } else { "" }
}

/// `date` input literals covering the infinities, BC dates, and AD years past
/// four digits.
pub(crate) fn date_literal() -> impl Strategy<Value = String> {
    prop_oneof![
        1 => select(&["infinity", "-infinity"][..]).prop_map(str::to_owned),
        8 => date_parts().prop_map(|(year, bc, month, day)| {
            format!("{year:04}-{month:02}-{day:02}{}", era_suffix(bc))
        }),
    ]
}

/// Formats a time of day with microsecond precision.
fn format_time(time: &NaiveTime) -> String {
    time.format("%H:%M:%S%.6f").to_string()
}

/// `time` input literals.
pub(crate) fn time_literal() -> impl Strategy<Value = String> {
    pg_time().prop_map(|time| format_time(&time))
}

/// Formats a Postgres `time with time zone` input literal.
///
/// Offsets cover the full Postgres range including a seconds component.
pub(crate) fn timetz_literal(time: &NaiveTime, offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let abs = offset_seconds.unsigned_abs();

    format!("{}{}{:02}:{:02}:{:02}", format_time(time), sign, abs / 3600, abs % 3600 / 60, abs % 60)
}

/// `timetz` input literals over the full offset range.
pub(crate) fn timetz_text_literal() -> impl Strategy<Value = String> {
    (pg_time(), -57_599i32..=57_599).prop_map(|(time, offset)| timetz_literal(&time, offset))
}

/// `timestamp` input literals covering the infinities and BC timestamps.
pub(crate) fn timestamp_literal() -> impl Strategy<Value = String> {
    prop_oneof![
        1 => select(&["infinity", "-infinity"][..]).prop_map(str::to_owned),
        8 => (date_parts(), pg_time()).prop_map(|((year, bc, month, day), time)| {
            format!("{year:04}-{month:02}-{day:02} {}{}", format_time(&time), era_suffix(bc))
        }),
    ]
}

/// `timestamptz` input literals with explicit offsets, covering the
/// infinities and BC timestamps.
///
/// Offsets stay within a day of UTC and the earliest BC year leaves room for
/// the shift to UTC, so every literal stays inside Postgres's range.
pub(crate) fn timestamptz_literal() -> impl Strategy<Value = String> {
    prop_oneof![
        1 => select(&["infinity", "-infinity"][..]).prop_map(str::to_owned),
        8 => (date_parts(), pg_time(), -50_399i32..=50_399).prop_map(
            |((year, bc, month, day), time, offset)| {
                let sign = if offset < 0 { '-' } else { '+' };
                let abs = offset.unsigned_abs();
                format!(
                    "{year:04}-{month:02}-{day:02} {}{sign}{:02}:{:02}{}",
                    format_time(&time),
                    abs / 3600,
                    abs % 3600 / 60,
                    era_suffix(bc)
                )
            }
        ),
    ]
}

/// `uuid` input literals.
pub(crate) fn uuid_literal() -> impl Strategy<Value = String> {
    any::<u128>().prop_map(|value| Uuid::from_u128(value).to_string())
}

/// `interval` input literals with mixed-sign fields and fractional seconds.
pub(crate) fn interval_literal() -> impl Strategy<Value = String> {
    (
        -10_000i32..=10_000,
        -1_000i32..=1_000,
        -100_000i32..=100_000,
        -1_000_000i64..=1_000_000,
        0u32..1_000_000,
    )
        .prop_map(|(years, months, days, seconds, micros)| {
            format!("{years} years {months} mons {days} days {seconds}.{micros:06} seconds")
        })
}

/// `inet` input literals for IPv4 and IPv6 with any prefix length.
pub(crate) fn inet_literal() -> impl Strategy<Value = String> {
    prop_oneof![
        (any::<[u8; 4]>(), 0u8..=32)
            .prop_map(|(octets, bits)| format!("{}/{bits}", Ipv4Addr::from(octets))),
        (any::<u128>(), 0u8..=128)
            .prop_map(|(address, bits)| format!("{}/{bits}", Ipv6Addr::from(address))),
    ]
}

/// `money` input literals over the full range except the most negative cent
/// count, which `cash_in` rejects because it parses the magnitude before
/// applying the sign.
pub(crate) fn money_literal() -> impl Strategy<Value = String> {
    (-i64::MAX..=i64::MAX).prop_map(|cents| {
        let sign = if cents < 0 { "-" } else { "" };
        let abs = cents.unsigned_abs();
        format!("{sign}{}.{:02}", abs / 100, abs % 100)
    })
}

/// `box` input literals.
pub(crate) fn box_literal() -> impl Strategy<Value = String> {
    any::<(i16, i16, i16, i16)>().prop_map(|(x1, y1, x2, y2)| format!("({x1},{y1}),({x2},{y2})"))
}

/// A generated JSON string whose characters each remember whether the
/// renderer writes them as a `\u` escape.
#[derive(Clone, Debug)]
pub(crate) struct JsonString(Vec<(char, bool)>);

impl JsonString {
    /// Builds a string rendered without optional escapes.
    fn plain(value: &str) -> Self {
        Self(value.chars().map(|c| (c, false)).collect())
    }

    /// Returns the decoded string value.
    fn value(&self) -> String {
        self.0.iter().map(|(c, _)| *c).collect()
    }

    /// Returns whether the string contains NUL, which `jsonb` rejects.
    fn contains_nul(&self) -> bool {
        self.0.iter().any(|(c, _)| *c == '\0')
    }

    /// Appends the quoted, escaped rendering to `out`.
    ///
    /// Characters outside the Basic Multilingual Plane become surrogate pairs
    /// when escaped.
    fn render(&self, out: &mut String) {
        out.push('"');
        for &(c, unicode_escape) in &self.0 {
            match c {
                _ if unicode_escape => {
                    let mut units = [0u16; 2];
                    for unit in c.encode_utf16(&mut units) {
                        write!(out, "\\u{unit:04x}").unwrap();
                    }
                }
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\t' => out.push_str("\\t"),
                '\r' => out.push_str("\\r"),
                '\u{8}' => out.push_str("\\b"),
                '\u{c}' => out.push_str("\\f"),
                c if (c as u32) < 0x20 => write!(out, "\\u{:04x}", c as u32).unwrap(),
                c => out.push(c),
            }
        }
        out.push('"');
    }
}

/// A generated JSON document tree.
///
/// Numbers keep their literal spelling and objects keep duplicate keys, so the
/// rendered text exercises everything `json` stores verbatim, while
/// [`JsonNode::matches`] knows the meaning `jsonb` assigns to it.
#[derive(Clone, Debug)]
pub(crate) enum JsonNode {
    /// `null`.
    Null,
    /// `true` or `false`.
    Bool(bool),
    /// A number literal.
    Number(String),
    /// A string.
    String(JsonString),
    /// An array.
    Array(Vec<JsonNode>),
    /// An object whose entries may repeat keys.
    Object(Vec<(JsonString, JsonNode)>),
}

impl JsonNode {
    /// Renders the document with `whitespace` between tokens.
    pub(crate) fn to_text(&self, whitespace: &str) -> String {
        let mut out = String::new();
        self.render(whitespace, &mut out);
        out
    }

    /// Appends the rendering to `out`.
    fn render(&self, whitespace: &str, out: &mut String) {
        match self {
            Self::Null => out.push_str("null"),
            Self::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
            Self::Number(literal) => out.push_str(literal),
            Self::String(value) => value.render(out),
            Self::Array(items) => {
                out.push('[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(whitespace);
                    item.render(whitespace, out);
                }
                out.push_str(whitespace);
                out.push(']');
            }
            Self::Object(entries) => {
                out.push('{');
                for (index, (key, value)) in entries.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(whitespace);
                    key.render(out);
                    out.push_str(whitespace);
                    out.push(':');
                    out.push_str(whitespace);
                    value.render(whitespace, out);
                }
                out.push_str(whitespace);
                out.push('}');
            }
        }
    }

    /// Returns whether any string or key contains NUL, which `jsonb` rejects
    /// while `json` accepts it as a `\u0000` escape.
    pub(crate) fn contains_nul(&self) -> bool {
        match self {
            Self::Null | Self::Bool(_) | Self::Number(_) => false,
            Self::String(value) => value.contains_nul(),
            Self::Array(items) => items.iter().any(Self::contains_nul),
            Self::Object(entries) => {
                entries.iter().any(|(key, value)| key.contains_nul() || value.contains_nul())
            }
        }
    }

    /// Returns whether `value` has the meaning `jsonb` assigns to this
    /// document.
    ///
    /// Numbers compare by numeric value, so `1e2`, `100`, and `100.0` match
    /// each other. Objects compare as key sets where a repeated key keeps its
    /// last value, like `jsonb`. Key order is irrelevant.
    pub(crate) fn matches(&self, value: &serde_json::Value) -> bool {
        use serde_json::Value;

        match (self, value) {
            (Self::Null, Value::Null) => true,
            (Self::Bool(expected), Value::Bool(actual)) => expected == actual,
            (Self::Number(expected), Value::Number(actual)) => {
                decimal_key(expected) == decimal_key(&actual.to_string())
            }
            (Self::String(expected), Value::String(actual)) => expected.value() == *actual,
            (Self::Array(expected), Value::Array(actual)) => {
                expected.len() == actual.len()
                    && expected
                        .iter()
                        .zip(actual)
                        .all(|(expected, actual)| expected.matches(actual))
            }
            (Self::Object(entries), Value::Object(actual)) => {
                let mut last_wins = BTreeMap::new();
                for (key, value) in entries {
                    last_wins.insert(key.value(), value);
                }

                last_wins.len() == actual.len()
                    && last_wins.iter().all(|(key, expected)| {
                        actual.get(key).is_some_and(|actual| expected.matches(actual))
                    })
            }
            _ => false,
        }
    }
}

/// Reduces a JSON number literal to its sign, significant digits, and decimal
/// exponent, so equal values compare equal regardless of spelling.
///
/// Zero has no sign, matching `jsonb`, which renders `-0` as `0`.
fn decimal_key(literal: &str) -> (bool, String, i64) {
    let (negative, unsigned) =
        literal.strip_prefix('-').map_or((false, literal), |unsigned| (true, unsigned));
    let (mantissa, exponent) = unsigned
        .split_once(['e', 'E'])
        .map_or((unsigned, 0), |(mantissa, exponent)| (mantissa, exponent.parse::<i64>().unwrap()));
    let (int_digits, frac_digits) = mantissa.split_once('.').unwrap_or((mantissa, ""));

    let mut digits = format!("{int_digits}{frac_digits}");
    let mut exponent = exponent - i64::try_from(frac_digits.len()).unwrap();
    while digits.ends_with('0') {
        digits.pop();
        exponent += 1;
    }
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return (false, String::new(), 0);
    }

    (negative, digits.to_owned(), exponent)
}

/// JSON number literals: integers, fractions, signed and unsigned exponents in
/// both cases, and huge exponents such as `1e1000` that overflow `f64`.
fn json_number() -> impl Strategy<Value = String> {
    let int_digits = prop_oneof![Just("0".to_owned()), "[1-9][0-9]{0,24}"];
    let exponent = (
        select(&["e", "E", "e+", "E+", "e-", "E-"][..]),
        prop_oneof![4 => 0u32..=20, 1 => select(&[307u32, 308, 309, 400, 1000][..])],
    );

    (any::<bool>(), int_digits, option::of("[0-9]{1,24}"), option::of(exponent)).prop_map(
        |(negative, int_digits, frac_digits, exponent)| {
            let mut literal = String::new();
            if negative {
                literal.push('-');
            }
            literal.push_str(&int_digits);
            if let Some(frac_digits) = frac_digits {
                literal.push('.');
                literal.push_str(&frac_digits);
            }
            if let Some((marker, value)) = exponent {
                literal.push_str(marker);
                literal.push_str(&value.to_string());
            }

            literal
        },
    )
}

/// JSON strings with escapes, surrogate pairs, and (when `allow_nul`) NUL.
fn json_string(allow_nul: bool) -> impl Strategy<Value = JsonString> {
    let char = prop_oneof![3 => any::<char>(), 1 => select(JSON_SPECIAL_CHARS)]
        .prop_filter("jsonb rejects NUL", move |c| allow_nul || *c != '\0');

    proptest::collection::vec((char, proptest::bool::weighted(0.2)), 0..=6).prop_map(JsonString)
}

/// Small JSON trees nested at most [`JSON_TREE_MAX_DEPTH`] containers deep.
///
/// Object keys are often drawn from a two-key pool so duplicate keys appear.
fn json_tree(allow_nul: bool) -> impl Strategy<Value = JsonNode> {
    let leaf = prop_oneof![
        Just(JsonNode::Null),
        any::<bool>().prop_map(JsonNode::Bool),
        json_number().prop_map(JsonNode::Number),
        json_string(allow_nul).prop_map(JsonNode::String),
    ];

    leaf.prop_recursive(JSON_TREE_MAX_DEPTH as u32, 24, 5, move |inner| {
        let key = prop_oneof![
            json_string(allow_nul),
            select(&["a", "b"][..]).prop_map(JsonString::plain),
        ];
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..=5).prop_map(JsonNode::Array),
            proptest::collection::vec((key, inner), 0..=5).prop_map(JsonNode::Object),
        ]
    })
}

/// JSON documents with the whitespace to render them with.
///
/// A random tree is wrapped in a chain of single-child arrays and objects whose
/// length is drawn from `chain`, so total nesting is up to the chain length
/// plus [`JSON_TREE_MAX_DEPTH`].
pub(crate) fn json_document(
    allow_nul: bool,
    chain: RangeInclusive<usize>,
) -> impl Strategy<Value = (JsonNode, &'static str)> {
    (json_tree(allow_nul), proptest::collection::vec(any::<bool>(), chain), select(JSON_WHITESPACE))
        .prop_map(|(tree, layers, whitespace)| {
            let document = layers.into_iter().fold(tree, |node, as_array| {
                if as_array {
                    JsonNode::Array(vec![node])
                } else {
                    JsonNode::Object(vec![(JsonString::plain("k"), node)])
                }
            });

            (document, whitespace)
        })
}

/// Shallow JSON input literals; `allow_nul` admits `\u0000` escapes, which
/// only `json` accepts.
pub(crate) fn json_literal(allow_nul: bool) -> impl Strategy<Value = String> {
    json_document(allow_nul, 0..=2).prop_map(|(document, whitespace)| document.to_text(whitespace))
}
