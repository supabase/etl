//! Configuration management for ETL applications.
//!
//! Provides environment detection, configuration loading from YAML files,
//! secret handling, and shared configuration types for various ETL services
//! and components.

mod environment;
mod load;
mod secret;
pub mod shared;

pub use environment::*;
pub use load::*;
pub use secret::*;
