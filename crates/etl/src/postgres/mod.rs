//! Postgres source, protocol, and migration support.

pub(crate) mod pool;
mod source_pool;
mod stream;

#[cfg(any(test, feature = "test-utils"))]
pub mod client;
#[cfg(not(any(test, feature = "test-utils")))]
#[expect(dead_code)]
pub(crate) mod client;
pub(crate) mod codec;
pub mod migrations;

pub(crate) use source_pool::OutOfBandSourcePool;
#[cfg(any(test, feature = "test-utils"))]
pub use stream::{FeedbackHandle, ReplicationMessageStream};
#[cfg(not(any(test, feature = "test-utils")))]
pub(crate) use stream::{FeedbackHandle, ReplicationMessageStream};
pub(crate) use stream::{TableCopyRow, TableCopyStream};
