mod connection;
mod core;
pub mod encoding;

pub use core::{RealtimeConfig, RealtimeDestination};

pub const DEFAULT_CHANNEL_PREFIX: &str = "etl";
pub const DEFAULT_MAX_RETRIES: u32 = 5;
