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

/// Postgres schema, type, and identifier model used by ETL.
pub mod types {
    pub use etl_postgres::types::*;
}

pub(crate) use replication_stream::{
    EventsStream, StatusUpdateResult, StatusUpdateType, TableCopyStream,
};
pub(crate) use source_pool::OutOfBandSourcePool;
