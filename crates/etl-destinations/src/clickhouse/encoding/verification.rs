//! Kani proofs for ClickHouse RowBinary length and UUID encoding.
//!
//! RowBinary has no field delimiters, so a wrong length prefix or UUID layout
//! shifts every later field of the insert.

use super::{ClickHouseValue, rb_encode_value, rb_varint};

/// Marks the buffer contents written before the value under test.
const SENTINEL: u8 = 0xAA;

/// Every `usize` encodes as minimal LEB128: one byte per started group of seven
/// bits, the continuation bit on every byte but the last, and the original
/// value when decoded. Bytes already in the buffer are kept.
#[kani::proof]
#[kani::unwind(11)]
fn varint_is_minimal_leb128() {
    let value: usize = kani::any();
    let mut buf = Vec::with_capacity(11);
    buf.push(SENTINEL);

    rb_varint(value, &mut buf);

    let [sentinel, encoded @ ..] = buf.as_slice() else {
        panic!("rb_varint dropped the existing buffer contents");
    };
    assert_eq!(*sentinel, SENTINEL);

    let significant_bits = usize::BITS - value.leading_zeros();
    assert_eq!(encoded.len(), usize::try_from(significant_bits.div_ceil(7).max(1)).unwrap());
    let (last, continued) = encoded.split_last().unwrap();
    assert!(*last < 0x80);
    assert!(continued.iter().all(|byte| *byte >= 0x80));

    let decoded = encoded
        .iter()
        .rev()
        .fold(0usize, |decoded, byte| (decoded << 7) | usize::from(byte & 0x7f));
    assert_eq!(decoded, value);
}

/// A UUID in standard big-endian byte order encodes as its high 64 bits, then
/// its low 64 bits, each little-endian.
///
/// `rb_encode_value` can build an `EtlError`, whose backtrace capture and
/// `Debug` formatting reach code Kani 0.68 cannot compile. The stub captures no
/// backtrace, and the result is checked with `is_ok` instead of `unwrap`, so
/// the error is never formatted; no property depends on either.
#[kani::proof]
#[kani::stub(std::backtrace::Backtrace::capture, std::backtrace::Backtrace::disabled)]
#[kani::unwind(17)]
fn uuid_encodes_as_little_endian_halves() {
    let uuid: [u8; 16] = kani::any();
    let mut buf = Vec::with_capacity(16);

    assert!(rb_encode_value(ClickHouseValue::Uuid(uuid), &mut buf).is_ok());

    let mut expected = uuid;
    expected[..8].reverse();
    expected[8..].reverse();
    assert_eq!(buf, expected);
}
