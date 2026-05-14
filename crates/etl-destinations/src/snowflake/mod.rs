mod auth;
mod client;
mod config;
mod core;
mod encoding;
mod error;
mod metrics;
mod schema;
mod sql_client;
mod streaming;

#[cfg(feature = "test-utils")]
pub mod test_utils;

pub use core::SnowflakeDestination;

pub use auth::{AuthManager, HttpExchanger, TokenExchanger, TokenProvider};
pub use client::SnowflakeClient;
pub use config::Config;
pub use encoding::{CdcMeta, CdcOperation};
pub use error::{Error, Result};
pub use sql_client::SqlClient;
pub use streaming::{
    ChannelHandle, ChannelStatusResponse, InsertRowsResponse, OffsetToken, OpenChannelResponse,
    RestStreamClient, RowBatch, RowBatchBuilder, StreamClient,
};
