//! BigQuery destination subcrate.
//!
//! Exposes the same public API under a `bigquery` module as in the
//! aggregator crate, to preserve existing import paths when reexported.

mod client;
mod core;
mod encoding;
mod encryption;
mod validation;

pub use client::{BigQueryDatasetId, BigQueryProjectId, BigQueryTableId};
pub use core::{BigQueryDestination, table_name_to_bigquery_table_id};
pub use encryption::install_crypto_provider_for_bigquery;

mod metrics;
