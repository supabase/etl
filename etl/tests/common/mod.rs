/// Common utilities and helpers for testing PostgreSQL replication functionality.
///
/// This module provides shared testing infrastructure including database management,
/// pipeline testing utilities, destination testing helpers, and table manipulation utilities.
/// It also includes common testing patterns like waiting for conditions to be met.
use std::time::{Duration, Instant};

#[cfg(feature = "bigquery")]
pub mod bigquery;
pub mod database;
pub mod destination;
pub mod event;
pub mod pipeline;
pub mod pipeline_v2;
pub mod state_store;
pub mod table;
pub mod test_destination_wrapper;
pub mod test_schema;

/// The maximum duration to wait for test conditions to be met.
///
/// This constant defines the timeout period for asynchronous test assertions,
/// ensuring tests don't hang indefinitely while waiting for expected states.
const MAX_ASSERTION_DURATION: Duration = Duration::from_secs(20);

/// The interval between condition checks during test assertions.
///
/// This constant defines how frequently we poll for condition changes while
/// waiting for test assertions to complete.
const ASSERTION_FREQUENCY_DURATION: Duration = Duration::from_millis(10);

/// Waits asynchronously for a condition to be met within the maximum timeout period.
///
/// This function repeatedly evaluates the provided condition until it returns true
/// or the maximum duration is exceeded. It's useful for testing asynchronous
/// operations where the exact completion time is not known.
///
/// # Panics
///
/// Panics if the condition is not met within [`MAX_ASSERTION_DURATION`].
pub async fn wait_for_condition<F>(condition: F)
where
    F: Fn() -> bool,
{
    let start = Instant::now();
    while start.elapsed() < MAX_ASSERTION_DURATION {
        if condition() {
            return;
        }

        tokio::time::sleep(ASSERTION_FREQUENCY_DURATION).await;
    }

    panic!("Failed to process all events within timeout")
}
