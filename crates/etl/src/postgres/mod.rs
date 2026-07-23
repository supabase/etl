//! Postgres source, protocol, and migration support.

mod source_pool;
mod stream;

#[cfg(any(test, feature = "test-utils"))]
pub mod client;
#[cfg(not(any(test, feature = "test-utils")))]
#[allow(dead_code, unused_imports)]
pub(crate) mod client;
pub(crate) mod codec;
pub mod migrations;

pub(crate) use source_pool::OutOfBandSourcePool;
pub(crate) use stream::{
    EventsStream, StatusUpdateResult, StatusUpdateType, TableCopyRow, TableCopyStream,
};
