mod auth;
mod client;
mod config;
mod core;
mod encoding;
mod error;
mod metrics;
mod schema;
mod sql;
mod sql_client;
mod streaming;

#[cfg(feature = "test-utils")]
pub mod test_utils;

use etl::schema::ColumnNameMapping;

/// Column-name mapping used at every Snowflake destination boundary.
///
/// Snowflake quoted identifiers preserve exact source names, so
/// `QUOTED_IDENTIFIERS_IGNORE_CASE` must remain disabled. Table creation, row
/// writes, schema planning, and recovery must use this same mapping. Changing it
/// for existing tables requires compatibility handling or a resync.
const SNOWFLAKE_COLUMN_NAME_MAPPING: ColumnNameMapping = ColumnNameMapping::Identity;

pub use core::Destination;

pub use auth::{AuthManager, HttpExchanger, TokenProvider};
pub use client::Client;
pub use config::Config;
pub use encoding::{CdcMeta, CdcOperation};
pub use error::{Error, Result, SnowpipeError};
pub use sql_client::SqlClient;
pub use streaming::{OffsetToken, RestStreamClient, RowBatch, RowBatchBuilder, StreamClient};
