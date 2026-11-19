//! Kani proofs for the fixed-layout date and time fast paths.
//!
//! Each proof covers every byte string of one length. A fast path that panics,
//! overflows, or stores a digit in the wrong component fails here. Shorter
//! inputs are rejected by the length checks before any indexing, so the
//! lengths below are the only ones a fast path can accept.

use std::str::from_utf8;

use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};
use etl_postgres::time::DATE_FORMAT;

use super::{parse_iso_date_fast, parse_iso_time_fast, parse_iso_timestamp_fast};

/// Length of `YYYY-MM-DD`.
const DATE_LEN: usize = 10;

/// Returns the decimal value of `bytes`, asserting that each one is a digit.
fn number(bytes: &[u8]) -> u32 {
    bytes.iter().fold(0, |value, &byte| {
        assert!(byte.is_ascii_digit());
        value * 10 + u32::from(byte - b'0')
    })
}

/// Asserts that `date` holds the components spelled by a `YYYY-MM-DD` input.
fn assert_date_matches(bytes: &[u8], date: NaiveDate) {
    assert_eq!(bytes.len(), DATE_LEN);
    assert_eq!(bytes[4], b'-');
    assert_eq!(bytes[7], b'-');
    assert_eq!(date.year(), i32::try_from(number(&bytes[..4])).unwrap());
    assert_eq!(date.month(), number(&bytes[5..7]));
    assert_eq!(date.day(), number(&bytes[8..10]));
}

/// Asserts that `time` holds the components spelled by a
/// `HH:MM:SS[.f{1,9}]` input.
fn assert_time_matches(bytes: &[u8], time: NaiveTime) {
    assert_eq!(bytes[2], b':');
    assert_eq!(bytes[5], b':');
    assert_eq!(time.hour(), number(&bytes[..2]));
    assert_eq!(time.minute(), number(&bytes[3..5]));
    assert_eq!(time.second(), number(&bytes[6..8]));

    let nanos = match bytes.get(8..) {
        Some([]) => 0,
        Some([b'.', fraction @ ..]) => {
            assert!((1..=9).contains(&fraction.len()));
            number(fraction) * 10u32.pow(9 - u32::try_from(fraction.len()).unwrap())
        }
        _ => panic!("time accepted with a non-fraction suffix"),
    };
    assert_eq!(time.nanosecond(), nanos);
}

/// Every accepted date holds exactly the year, month, and day its digits
/// spell, and no input panics.
#[kani::proof]
fn date_fast_path_matches_its_digits() {
    let bytes: [u8; DATE_LEN] = kani::any();

    if let Some(date) = parse_iso_date_fast(&bytes) {
        assert_date_matches(&bytes, date);
    }
}

/// Every date the fast path accepts, chrono accepts with the same value, so
/// taking the fast path never changes a parse result.
#[kani::proof]
#[kani::unwind(12)]
fn date_fast_path_agrees_with_chrono() {
    let bytes: [u8; DATE_LEN] = kani::any();

    if let Some(date) = parse_iso_date_fast(&bytes) {
        let value = from_utf8(&bytes).unwrap();
        assert_eq!(NaiveDate::parse_from_str(value, DATE_FORMAT), Ok(date));
    }
}

/// Checks every `N`-byte time input: no panic, and an accepted time holds
/// exactly the hour, minute, second, and fraction its digits spell.
fn check_time<const N: usize>() {
    let bytes: [u8; N] = kani::any();

    if let Some(time) = parse_iso_time_fast(&bytes) {
        assert_time_matches(&bytes, time);
    }
}

/// Checks every `N`-byte timestamp input: no panic, and an accepted timestamp
/// joins the date and time its digits spell.
fn check_timestamp<const N: usize>() {
    let bytes: [u8; N] = kani::any();

    if let Some(timestamp) = parse_iso_timestamp_fast(&bytes) {
        assert_eq!(bytes[DATE_LEN], b' ');
        assert_date_matches(&bytes[..DATE_LEN], timestamp.date());
        assert_time_matches(&bytes[DATE_LEN + 1..], timestamp.time());
    }
}

/// Declares one proof per length, each running `$check::<length>`.
macro_rules! length_proofs {
    ($check:ident: $($name:ident = $len:literal),+ $(,)?) => {
        $(
            #[doc = concat!("Runs `", stringify!($check), "` for length ", stringify!($len), ".")]
            #[kani::proof]
            #[kani::unwind(12)]
            fn $name() {
                $check::<$len>();
            }
        )+
    };
}

length_proofs!(check_time:
    time_len_8 = 8,
    time_len_9 = 9,
    time_len_10 = 10,
    time_len_11 = 11,
    time_len_12 = 12,
    time_len_13 = 13,
    time_len_14 = 14,
    time_len_15 = 15,
    time_len_16 = 16,
    time_len_17 = 17,
    time_len_18 = 18,
    time_len_19 = 19,
);

length_proofs!(check_timestamp:
    timestamp_len_19 = 19,
    timestamp_len_20 = 20,
    timestamp_len_21 = 21,
    timestamp_len_22 = 22,
    timestamp_len_23 = 23,
    timestamp_len_24 = 24,
    timestamp_len_25 = 25,
    timestamp_len_26 = 26,
    timestamp_len_27 = 27,
    timestamp_len_28 = 28,
    timestamp_len_29 = 29,
    timestamp_len_30 = 30,
);
