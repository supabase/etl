//! Budget-based property test runner.
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

use std::time::{Duration, Instant};

use proptest::{
    strategy::Strategy,
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
