//! Kani proofs for stripping the dimensions prefix from Postgres array text.
//!
//! Every array cell passes through [`strip_array_dimensions_prefix`], so
//! stripping too much or too little shifts or merges array elements. Each proof
//! covers every input of one length drawn from [`in_array_alphabet`].

use std::str::from_utf8;

use super::strip_array_dimensions_prefix;

/// Returns whether `byte` is one the proofs draw from: all of the dimensions
/// prefix syntax, the bytes just outside `0..=9`, the start of an array body,
/// and the two UTF-8 bytes of `é`, so multibyte characters follow the prefix.
///
/// A `matches!` keeps this loop-free, so it needs no unwinding.
fn in_array_alphabet(byte: u8) -> bool {
    matches!(byte, b'[' | b']' | b':' | b'-' | b'=' | b'0' | b'9' | b'/' | b'{' | 0xC3 | 0xA9)
}

/// Returns whether `bytes` is an optionally negative decimal integer.
fn is_integer(bytes: &[u8]) -> bool {
    let digits = bytes.strip_prefix(b"-").unwrap_or(bytes);
    !digits.is_empty() && digits.iter().all(u8::is_ascii_digit)
}

/// Returns whether `prefix` is exactly one `[lower:upper]=` dimension group.
fn is_one_dimension_prefix(prefix: &[u8]) -> bool {
    let [b'[', bounds @ .., b']', b'='] = prefix else {
        return false;
    };
    let mut parts = bounds.split(|byte| *byte == b':');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(lower), Some(upper), None) => is_integer(lower) && is_integer(upper),
        _ => false,
    }
}

/// Checks every `N`-byte input: input without a leading `[` is returned
/// unchanged, and otherwise exactly one well-formed dimension group and its
/// `=` are removed, with every other prefix rejected.
fn check_dimensions_prefix<const N: usize>() {
    let bytes: [u8; N] = kani::any();
    kani::assume(bytes.iter().all(|byte| in_array_alphabet(*byte)));
    let Ok(input) = from_utf8(&bytes) else {
        return;
    };

    // Matching instead of `unwrap` keeps `EtlError`'s `Debug` formatting, which
    // Kani 0.68 cannot compile, out of the proof. Forgetting each error skips
    // its drop glue, whose backtrace and nested-error cleanup otherwise
    // dominate the proof's cost; no property depends on either.
    let result = strip_array_dimensions_prefix(input).map_err(std::mem::forget);

    if bytes.first() != Some(&b'[') {
        assert_eq!(result, Ok(input));
        return;
    }

    // No dimension group contains `=`, so a prefix ends at the first one.
    let prefix_len = bytes.iter().position(|byte| *byte == b'=').map(|index| index + 1);
    let expected = prefix_len.filter(|len| is_one_dimension_prefix(&bytes[..*len]));
    match (result, expected) {
        (Ok(rest), Some(len)) => assert_eq!(rest.as_bytes(), &bytes[len..]),
        (Err(()), None) => {}
        (Ok(_), None) => panic!("malformed dimensions prefix accepted"),
        (Err(()), Some(_)) => panic!("well-formed dimensions prefix rejected"),
    }
}

/// Declares one proof per input length, each running
/// [`check_dimensions_prefix`].
///
/// Each proof's unwind bound is its length plus two, enough for every loop
/// over the input. Rejected input builds an `EtlError`; the stub skips its
/// backtrace capture, which Kani 0.68 cannot compile, and no property depends
/// on the backtrace.
macro_rules! dimensions_prefix_proofs {
    ($($name:ident: $len:literal, unwind $unwind:literal;)+) => {
        $(
            #[doc = concat!("Checks every ", stringify!($len), "-byte array input.")]
            #[kani::proof]
            #[kani::stub(std::backtrace::Backtrace::capture, std::backtrace::Backtrace::disabled)]
            #[kani::unwind($unwind)]
            fn $name() {
                check_dimensions_prefix::<$len>();
            }
        )+
    };
}

dimensions_prefix_proofs! {
    array_prefix_len_0: 0, unwind 2;
    array_prefix_len_1: 1, unwind 3;
    array_prefix_len_2: 2, unwind 4;
    array_prefix_len_3: 3, unwind 5;
    array_prefix_len_4: 4, unwind 6;
    array_prefix_len_5: 5, unwind 7;
    array_prefix_len_6: 6, unwind 8;
    array_prefix_len_7: 7, unwind 9;
    array_prefix_len_8: 8, unwind 10;
    array_prefix_len_9: 9, unwind 11;
    array_prefix_len_10: 10, unwind 12;
    array_prefix_len_11: 11, unwind 13;
}
