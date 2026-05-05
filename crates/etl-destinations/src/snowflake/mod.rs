mod auth;
mod channel;
mod config;
mod core;
mod encoding;
mod error;
mod metrics;
mod schema;
mod sql_client;
mod streaming_client;

#[cfg(feature = "test-utils")]
pub mod test_utils;

pub use core::{DefaultSnowflakeDestination, SnowflakeDestination};

pub use auth::{AuthManager, HttpExchanger, TokenExchanger, TokenProvider};
pub use channel::{ChannelHandle, ChannelStatus, OffsetToken};
pub use config::Config;
pub use encoding::{CdcOperation, RowBatch};
pub use error::{Error, Result};
pub use sql_client::SqlClient;
pub use streaming_client::{
    ChannelStatusResponse, InsertRowsResponse, OpenChannelResponse, RestStreamClient, StreamClient,
};
