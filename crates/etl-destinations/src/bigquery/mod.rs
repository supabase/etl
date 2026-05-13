mod client;
mod compatibility;
mod core;
mod encoding;
mod metrics;
#[cfg(feature = "test-utils")]
pub mod test_utils;

#[cfg(feature = "test-utils")]
pub use core::table_name_to_bigquery_table_id;
pub use core::{BigQueryDestination, BigQueryDestinationOptions};

pub use client::{BigQueryClient, BigQueryDatasetId, BigQueryProjectId, BigQueryTableId};
