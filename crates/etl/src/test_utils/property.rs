//! Budget-based property test runner and shared roundtrip-test helpers.
//!
//! [`run_property`] runs freshly generated cases until a wall-clock budget
//! elapses instead of a fixed case count. The budget comes from the
//! `PROPERTY_TEST_BUDGET_SECS` environment variable (default 2), so a
//! regular suite pays a few seconds per property while CI or local deep runs
//! can raise it to minutes.
//!
//! On failure the panic names the failing chunk's RNG seed; set
//! `PROPERTY_TEST_SEED` to that value to replay the chunk
//! deterministically.
//!
//! The module also hosts the small helpers the value roundtrip properties
//! share across crates: the sync-to-async bridge, bit-level float matchers,
//! and strategies for values inside the Postgres envelope.

use std::time::{Duration, Instant};

use chrono::NaiveTime;
use proptest::{
    arbitrary::any,
    prop_oneof,
    strategy::{Just, Strategy},
    test_runner::{Config, RngAlgorithm, TestCaseError, TestRng, TestRunner},
};

/// Number of generated cases between deadline checks.
const CASES_PER_CHUNK: u32 = 64;

/// Returns the wall-clock budget for one property.
///
/// Panics on a malformed value instead of falling back to the default, so a
/// deep run with a typoed budget fails loudly rather than silently running
/// the shallow default.
fn property_budget() -> Duration {
    let secs = std::env::var("PROPERTY_TEST_BUDGET_SECS").map_or(2, |value| {
        value.parse().expect("PROPERTY_TEST_BUDGET_SECS must be an integer number of seconds")
    });

    Duration::from_secs(secs)
}

/// Returns the pinned chunk seed from `PROPERTY_TEST_SEED`, if set.
fn replay_seed() -> Option<u64> {
    let value = std::env::var("PROPERTY_TEST_SEED").ok()?;
    Some(value.parse().expect("PROPERTY_TEST_SEED must be a u64 seed"))
}

/// Builds the deterministic RNG for one chunk of cases from a `u64` seed.
fn chunk_rng(seed: u64) -> TestRng {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    TestRng::from_seed(RngAlgorithm::ChaCha, &bytes)
}

/// Runs `check` on freshly generated values until the property budget elapses.
///
/// Each chunk runs a fixed number of cases (`CASES_PER_CHUNK`) from a fresh
/// random seed. On failure the value is shrunk by proptest and reported
/// through a panic that also names the chunk seed, so the minimal failing
/// input shows up in the test output and the chunk can be replayed with
/// `PROPERTY_TEST_SEED`.
pub fn run_property<S: Strategy>(
    name: &str,
    strategy: &S,
    check: impl Fn(&S::Value) -> Result<(), TestCaseError>,
) {
    let deadline = Instant::now() + property_budget();
    let replay_seed = replay_seed();
    let mut total_cases = 0u64;

    loop {
        let seed = replay_seed.unwrap_or_else(rand::random);
        let config =
            Config { cases: CASES_PER_CHUNK, failure_persistence: None, ..Default::default() };
        let mut runner = TestRunner::new_with_rng(config, chunk_rng(seed));

        match runner.run(strategy, |value| check(&value)) {
            Ok(()) => total_cases += u64::from(CASES_PER_CHUNK),
            Err(err) => panic!(
                "property '{name}' failed after ~{total_cases} passing cases; rerun with \
                 PROPERTY_TEST_SEED={seed} to replay the failing chunk:\n{err}"
            ),
        }

        // A pinned replay seed regenerates the same cases, so it runs exactly
        // one chunk.
        if replay_seed.is_some() || Instant::now() >= deadline {
            break;
        }
    }

    println!("property '{name}': {total_cases} cases passed");
}

/// Runs an async future to completion from inside a synchronous proptest
/// closure.
///
/// Properties execute on a multi-threaded Tokio runtime worker, so blocking
/// in place is safe and keeps background I/O (database connections, HTTP
/// clients) driven.
pub fn block_on<F: Future>(future: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
}

/// Compares floats bit-for-bit, treating every NaN payload as equal.
///
/// Sign and value bits of non-NaN floats (including signed zeros) must
/// survive a roundtrip exactly; NaN payloads are only required to stay NaN,
/// since not every transport preserves them.
pub fn f64_matches(expected: f64, parsed: f64) -> bool {
    if expected.is_nan() { parsed.is_nan() } else { expected.to_bits() == parsed.to_bits() }
}

/// [`f64_matches`] for `f32`.
pub fn f32_matches(expected: f32, parsed: f32) -> bool {
    if expected.is_nan() { parsed.is_nan() } else { expected.to_bits() == parsed.to_bits() }
}

/// Optional-aware [`f64_matches`].
pub fn opt_f64_matches(expected: Option<f64>, parsed: Option<f64>) -> bool {
    match (expected, parsed) {
        (None, None) => true,
        (Some(expected), Some(parsed)) => f64_matches(expected, parsed),
        _ => false,
    }
}

/// Optional-aware [`f32_matches`].
pub fn opt_f32_matches(expected: Option<f32>, parsed: Option<f32>) -> bool {
    match (expected, parsed) {
        (None, None) => true,
        (Some(expected), Some(parsed)) => f32_matches(expected, parsed),
        _ => false,
    }
}

/// All `f64` bit patterns: normals, subnormals, zeros, infinities, NaNs.
pub fn any_f64() -> impl Strategy<Value = f64> {
    any::<u64>().prop_map(f64::from_bits)
}

/// All `f32` bit patterns: normals, subnormals, zeros, infinities, NaNs.
pub fn any_f32() -> impl Strategy<Value = f32> {
    any::<u32>().prop_map(f32::from_bits)
}

/// Strings that are valid Postgres `text` values (no NUL bytes).
pub fn pg_text() -> impl Strategy<Value = String> {
    any::<String>().prop_filter("postgres text cannot contain NUL", |s| !s.contains('\0'))
}

/// Microsecond-precision times of day, matching Postgres's time resolution.
///
/// Fractional seconds are biased toward the rendering boundaries (whole
/// seconds and whole milliseconds) so format-dependent consumers exercise
/// every fraction shape instead of almost always drawing six-digit
/// fractions.
///
/// Postgres also accepts and stores `24:00:00`, but `chrono::NaiveTime`
/// cannot represent it and the codec rejects it (a pinned known gap; see
/// `time_falls_back_for_non_iso_shapes` in the codec tests). It stays
/// outside the envelope until the codec and destinations decide how to
/// carry it.
pub fn pg_time() -> impl Strategy<Value = NaiveTime> {
    let micros = prop_oneof![
        8 => 0u32..1_000_000,
        1 => (0u32..1_000).prop_map(|millis| millis * 1_000),
        1 => Just(0u32),
    ];

    (0u32..86_400, micros).prop_map(|(seconds, micros)| {
        NaiveTime::from_num_seconds_from_midnight_opt(seconds, micros * 1_000).unwrap()
    })
}
