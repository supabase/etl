pub mod clients;
pub mod concurrency;
pub mod conversions;
pub mod destination;
pub mod encryption;
pub mod error;
#[cfg(feature = "failpoints")]
pub mod failpoints;
pub mod pipeline;
pub mod replication;
pub mod schema;
pub mod state;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
pub mod workers;

// Re-export main error types for convenience
pub use error::{ETLError, ETLResult, ErrorKind};
