mod config;
mod core;
mod schema;

pub use config::S3Config;
pub use core::{DuckLakeDestination, table_name_to_ducklake_table_name};
