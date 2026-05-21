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

pub use core::Destination;

pub use auth::{AuthManager, HttpExchanger, TokenProvider};
pub use client::Client;
pub use config::Config;
pub use encoding::{CdcMeta, CdcOperation};
pub use error::{Error, Result};
pub use sql_client::SqlClient;
pub use streaming::{OffsetToken, RestStreamClient, RowBatch, RowBatchBuilder, StreamClient};
