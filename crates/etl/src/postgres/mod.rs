//! Postgres source, protocol, and migration support.

mod replication_stream;
mod source_pool;

#[cfg(any(test, feature = "test-utils"))]
pub mod client;
#[cfg(not(any(test, feature = "test-utils")))]
#[allow(dead_code, unused_imports)]
pub(crate) mod client;
pub(crate) mod codec;
pub mod migrations;

pub(crate) use replication_stream::{
    EventsStream, StatusUpdateResult, StatusUpdateType, TableCopyStream,
};
pub(crate) use source_pool::OutOfBandSourcePool;
