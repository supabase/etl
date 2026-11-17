//! Deterministic fixture generation and output checksums.

use std::fmt::{self, Debug, Write};

use chrono::{DateTime, Duration, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};

/// FNV-1a offset basis for 64-bit hashes.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a prime for 64-bit hashes.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Days from 1970-01-01 covered by generated dates, ending in 2049.
const DATE_RANGE_DAYS: u64 = 29_200;

/// Seconds in one day.
const SECONDS_PER_DAY: u64 = 86_400;

/// Incremental FNV-1a hash that accepts formatted output without allocating.
pub(super) struct Checksum(u64);

impl Checksum {
    /// Starts an empty hash.
    pub(super) fn new() -> Self {
        Self(FNV_OFFSET_BASIS)
    }

    /// Adds raw bytes to the hash.
    pub(super) fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0 ^ u64::from(*byte)).wrapping_mul(FNV_PRIME);
        }
    }

    /// Returns the hash of everything added so far.
    pub(super) fn finish(&self) -> u64 {
        self.0
    }
}

impl Write for Checksum {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.update(value.as_bytes());

        Ok(())
    }
}

/// Hashes raw output bytes.
pub(super) fn checksum_bytes(bytes: &[u8]) -> u64 {
    let mut checksum = Checksum::new();
    checksum.update(bytes);

    checksum.finish()
}

/// Hashes the `Debug` rendering of every item, so any changed value changes the
/// checksum.
pub(super) fn checksum_debug<T: Debug>(items: &[T]) -> u64 {
    let mut checksum = Checksum::new();
    for item in items {
        writeln!(checksum, "{item:?}").expect("hashing formatted output cannot fail");
    }

    checksum.finish()
}

/// SplitMix64 generator, so fixtures depend only on the seed and scenario
/// name.
pub(super) struct Rng(u64);

impl Rng {
    /// Creates a generator whose stream is specific to `scenario`.
    pub(super) fn for_scenario(seed: u64, scenario: &str) -> Self {
        Self(seed ^ checksum_bytes(scenario.as_bytes()))
    }

    /// Returns the next 64 random bits.
    pub(super) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);

        value ^ (value >> 31)
    }

    /// Returns a value in `0..bound`. The slight modulo bias does not matter
    /// for fixtures.
    pub(super) fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound
    }

    /// Returns `true` once in `one_in` calls on average.
    pub(super) fn one_in(&mut self, one_in: u64) -> bool {
        self.below(one_in) == 0
    }

    /// Appends `count` decimal digits, the first of which is never zero.
    pub(super) fn push_digits(&mut self, output: &mut String, count: usize) {
        for index in 0..count {
            let digit = if index == 0 { 1 + self.below(9) } else { self.below(10) };
            output.push(char::from(b'0' + digit as u8));
        }
    }

    /// Appends `count` lowercase ASCII letters.
    pub(super) fn push_letters(&mut self, output: &mut String, count: usize) {
        for _ in 0..count {
            output.push(char::from(b'a' + self.below(26) as u8));
        }
    }

    /// Appends `count` random bytes as lowercase hex.
    pub(super) fn push_hex_bytes(&mut self, output: &mut String, count: usize) {
        for _ in 0..count {
            write!(output, "{:02x}", self.below(256)).expect("writing to a String cannot fail");
        }
    }

    /// Returns Postgres numeric text shaped like common production columns:
    /// mostly two-decimal amounts, plus integers, small fractions, negatives,
    /// and a tail of high-precision values.
    pub(super) fn numeric_text(&mut self) -> String {
        let mut text = String::new();
        match self.below(10) {
            0..=3 => {
                let digits = 1 + self.below(9) as usize;
                self.push_digits(&mut text, digits);
                text.push('.');
                self.push_digits(&mut text, 2);
            }
            4..=5 => {
                let digits = 1 + self.below(18) as usize;
                self.push_digits(&mut text, digits);
            }
            6..=7 => {
                text.push_str("0.");
                let digits = 4 + self.below(7) as usize;
                self.push_digits(&mut text, digits);
            }
            8 => {
                text.push('-');
                let digits = 1 + self.below(9) as usize;
                self.push_digits(&mut text, digits);
                text.push('.');
                self.push_digits(&mut text, 2);
            }
            _ => {
                self.push_digits(&mut text, 20);
                text.push('.');
                self.push_digits(&mut text, 18);
            }
        }

        text
    }

    /// Returns a date between 1970 and 2049.
    pub(super) fn date(&mut self) -> NaiveDate {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("the Unix epoch is a valid date");

        epoch + Duration::days(self.below(DATE_RANGE_DAYS) as i64)
    }

    /// Returns a time of day; half of the values carry microseconds, as
    /// `now()` timestamps do, and half are whole seconds.
    pub(super) fn time(&mut self) -> NaiveTime {
        let seconds = self.below(SECONDS_PER_DAY) as u32;
        let microseconds = if self.one_in(2) { self.below(1_000_000) as u32 } else { 0 };

        NaiveTime::from_num_seconds_from_midnight_opt(seconds, microseconds * 1_000)
            .expect("seconds and nanoseconds are within one day")
    }

    /// Returns a timestamp without a time zone.
    pub(super) fn timestamp(&mut self) -> NaiveDateTime {
        NaiveDateTime::new(self.date(), self.time())
    }

    /// Returns a UTC timestamp.
    pub(super) fn timestamptz(&mut self) -> DateTime<Utc> {
        self.timestamp().and_utc()
    }

    /// Returns a UTC offset on a quarter hour within fourteen hours of UTC.
    pub(super) fn utc_offset(&mut self) -> FixedOffset {
        let quarter_hours = self.below(113) as i32 - 56;

        FixedOffset::east_opt(quarter_hours * 15 * 60)
            .expect("offsets within fourteen hours are valid")
    }
}
