//! Reusable Postgres integration primitives for ETL crates.
//!
//! This crate owns Postgres-specific helpers that need to be shared across
//! crates: source database access, replication slot naming, ETL metadata-store
//! queries, schema primitives, value wrappers, and codec support types.

pub mod default_expression;
pub mod lag;
pub mod numeric;
pub mod schema;
pub mod slots;
pub mod source;
#[doc(hidden)]
#[cfg(feature = "sqlx")]
pub mod sqlx;
pub mod store;
#[doc(hidden)]
#[cfg(feature = "test-utils")]
pub mod test_utils;
pub mod time;
#[cfg(feature = "tokio")]
pub mod tokio;
pub mod type_utils;
pub mod version;
