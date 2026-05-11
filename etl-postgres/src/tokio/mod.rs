pub mod cleanup;
mod client;
pub mod db;
pub mod health;
pub mod lag;
mod rustls;
pub mod slots;
pub mod store;
mod timed_client;

pub use client::{PgSourceClient, PgSourceError, PgSourceTransaction};
pub use rustls::MakeRustlsConnect;
pub use timed_client::{DEFAULT_IDLE_TIMEOUT, TimedPgSourceClient, TimedPgSourceClientLease};

#[cfg(feature = "test-utils")]
pub mod test_utils;
