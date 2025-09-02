//! Iceberg destination subcrate.
//!
//! Exposes the same public API under an `iceberg` module as in the
//! aggregator crate, to preserve existing import paths when reexported.

mod client;
mod schema;

pub use client::IcebergClient;
