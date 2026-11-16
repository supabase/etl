//! Kani proofs for Postgres bytea hex decoding.
//!
//! Every `bytea` value in copy and streaming rows passes through this decoder,
//! so a wrong nibble silently corrupts binary columns. Each proof covers every
//! UTF-8 input of one length.

use std::str::from_utf8;

use super::parse_bytea_hex_string;

/// Returns the value of an ASCII hex digit in either case.
fn hex_value(byte: u8) -> Option<u8> {
    char::from(byte).to_digit(16).map(|digit| u8::try_from(digit).unwrap())
}

/// Checks every `N`-byte input: decoding succeeds exactly for `\x` followed by
/// an even number of hex digits, and each decoded byte is the value of its
/// digit pair.
fn check_bytea_hex<const N: usize>() {
    let bytes: [u8; N] = kani::any();
    let Ok(value) = from_utf8(&bytes) else {
        return;
    };

    // Forgetting the error skips `EtlError`'s drop glue, whose backtrace and
    // nested-error cleanup otherwise dominate the proof's cost; no property
    // depends on it.
    let result = parse_bytea_hex_string(value).map_err(std::mem::forget);

    let digits = bytes.strip_prefix(b"\\x");
    let well_formed = digits.is_some_and(|digits| {
        digits.len().is_multiple_of(2) && digits.iter().all(|byte| hex_value(*byte).is_some())
    });
    assert_eq!(result.is_ok(), well_formed);

    if let (Ok(decoded), Some(digits)) = (result, digits) {
        assert_eq!(decoded.len(), digits.len() / 2);
        for (byte, pair) in decoded.iter().zip(digits.chunks_exact(2)) {
            assert_eq!(*byte, hex_value(pair[0]).unwrap() * 16 + hex_value(pair[1]).unwrap());
        }
    }
}

/// Declares one proof per input length, each running [`check_bytea_hex`].
///
/// Each proof's unwind bound is its length plus two, enough for every loop
/// over the input. Rejected input builds an `EtlError`; the stub skips its
/// backtrace capture, which Kani 0.68 cannot compile, and no property depends
/// on the backtrace.
macro_rules! bytea_hex_proofs {
    ($($name:ident: $len:literal, unwind $unwind:literal;)+) => {
        $(
            #[doc = concat!("Checks every ", stringify!($len), "-byte bytea hex input.")]
            #[kani::proof]
            #[kani::stub(std::backtrace::Backtrace::capture, std::backtrace::Backtrace::disabled)]
            #[kani::unwind($unwind)]
            fn $name() {
                check_bytea_hex::<$len>();
            }
        )+
    };
}

bytea_hex_proofs! {
    bytea_hex_len_0: 0, unwind 2;
    bytea_hex_len_1: 1, unwind 3;
    bytea_hex_len_2: 2, unwind 4;
    bytea_hex_len_3: 3, unwind 5;
    bytea_hex_len_4: 4, unwind 6;
    bytea_hex_len_5: 5, unwind 7;
    bytea_hex_len_6: 6, unwind 8;
    bytea_hex_len_7: 7, unwind 9;
    bytea_hex_len_8: 8, unwind 10;
}
