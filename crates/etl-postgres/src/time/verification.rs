//! Kani proofs for numeric UTC offset parsing.
//!
//! `timestamptz` values are normalized to UTC with these offsets, so a wrong
//! offset silently shifts every replicated value. Each proof covers every input
//! of one length drawn from [`in_offset_alphabet`]; together they cover lengths
//! up to 10 bytes, one more than the longest accepted layout, `+HH:MM:SS`.

use std::str::from_utf8;

use super::{POSTGRES_UTC_OFFSET_LIMIT_SECONDS, parse_postgres_utc_offset};

/// Returns whether `byte` is one the proofs draw from: all of offset syntax,
/// `x` standing in for any other ASCII byte, and the two UTF-8 bytes of `é`, so
/// multibyte characters reach the parser's slicing.
///
/// A `matches!` keeps this loop-free, so it needs no unwinding.
fn in_offset_alphabet(byte: u8) -> bool {
    matches!(byte, b'0'..=b'9' | b'+' | b'-' | b':' | b'x' | 0xC3 | 0xA9)
}

/// Returns the value of two ASCII digits, asserting that both are digits.
fn two_digits(bytes: &[u8]) -> i32 {
    let [high, low] = bytes else {
        panic!("offset component is not two bytes");
    };
    assert!(high.is_ascii_digit() && low.is_ascii_digit());
    i32::from(high - b'0') * 10 + i32::from(low - b'0')
}

/// Checks every `N`-byte input: no panic, and an accepted offset holds exactly
/// the signed hours, minutes, and seconds its digits spell, within the Postgres
/// limit.
fn check_offset<const N: usize>() {
    let bytes: [u8; N] = kani::any();
    kani::assume(bytes.iter().all(|byte| in_offset_alphabet(*byte)));
    let Ok(value) = from_utf8(&bytes) else {
        return;
    };

    let Some(offset) = parse_postgres_utc_offset(value) else {
        return;
    };

    let sign = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => panic!("offset accepted without a sign"),
    };
    let (hours, minutes, seconds) = match bytes[1..] {
        [_, _] => (two_digits(&bytes[1..3]), 0, 0),
        [_, _, _, _] => (two_digits(&bytes[1..3]), two_digits(&bytes[3..5]), 0),
        [_, _, b':', _, _] => (two_digits(&bytes[1..3]), two_digits(&bytes[4..6]), 0),
        [_, _, _, _, _, _] => {
            (two_digits(&bytes[1..3]), two_digits(&bytes[3..5]), two_digits(&bytes[5..7]))
        }
        [_, _, b':', _, _, b':', _, _] => {
            (two_digits(&bytes[1..3]), two_digits(&bytes[4..6]), two_digits(&bytes[7..9]))
        }
        _ => panic!("offset accepted in an unsupported layout"),
    };

    assert!(minutes < 60 && seconds < 60);
    let total_seconds = hours * 3600 + minutes * 60 + seconds;
    assert!(total_seconds < POSTGRES_UTC_OFFSET_LIMIT_SECONDS);
    assert_eq!(offset.local_minus_utc(), sign * total_seconds);
}

/// Finds `needle` in `haystack` one byte at a time.
///
/// Stands in for `core`'s word-at-a-time `memchr`, whose alignment-dependent
/// loops Kani cannot bound by the input length.
fn bytewise_memchr(needle: u8, haystack: &[u8]) -> Option<usize> {
    haystack.iter().position(|byte| *byte == needle)
}

/// Declares one proof per input length, each running [`check_offset`].
///
/// Each proof's unwind bound is its length plus two, enough for every loop
/// over the input; a tighter bound keeps the standard library's UTF-8 and
/// `memchr` loops from unrolling past the input.
macro_rules! offset_proofs {
    ($($name:ident: $len:literal, unwind $unwind:literal;)+) => {
        $(
            #[doc = concat!("Checks every ", stringify!($len), "-byte UTC offset input.")]
            #[kani::proof]
            #[kani::stub(core::slice::memchr::memchr, bytewise_memchr)]
            #[kani::unwind($unwind)]
            fn $name() {
                check_offset::<$len>();
            }
        )+
    };
}

offset_proofs! {
    utc_offset_len_0: 0, unwind 2;
    utc_offset_len_1: 1, unwind 3;
    utc_offset_len_2: 2, unwind 4;
    utc_offset_len_3: 3, unwind 5;
    utc_offset_len_4: 4, unwind 6;
    utc_offset_len_5: 5, unwind 7;
    utc_offset_len_6: 6, unwind 8;
    utc_offset_len_7: 7, unwind 9;
    utc_offset_len_8: 8, unwind 10;
    utc_offset_len_9: 9, unwind 11;
    utc_offset_len_10: 10, unwind 12;
}
