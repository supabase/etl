mod client;
mod core;
mod encoding;
pub(crate) mod initial_copy;
mod metrics;
mod schema;
mod sql;
#[cfg(feature = "test-utils")]
pub mod test_utils;
pub(crate) mod validation;

pub use core::BigQueryDestination;
#[cfg(feature = "test-utils")]
pub use core::table_name_to_bigquery_table_id;

pub use client::{BigQueryClient, BigQueryDatasetId, BigQueryProjectId, BigQueryTableId};
