//! Configuration management for ETL applications.
//!
//! Provides environment detection, configuration loading from YAML files,
//! secret handling, and shared configuration types for various ETL services
//! and components.

mod ducklake;
mod environment;
mod load;
mod secret;
pub mod shared;

pub use ducklake::{ParseDucklakeUrlError, parse_ducklake_url};
pub use environment::Environment;
pub use load::{Config, LoadConfigError, load_config};
pub use secret::SerializableSecretString;
