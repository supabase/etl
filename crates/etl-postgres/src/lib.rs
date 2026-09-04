//! Reusable Postgres integration primitives for ETL crates.
//!
//! This crate owns Postgres-specific helpers that need to be shared across
//! crates: source database access, replication slot naming, ETL metadata-store
//! queries, schema primitives, value wrappers, and type conversion helpers.
//!
//! Select the rustls cryptography backend with the `tls-rustls-ring` (default)
//! or `tls-rustls-aws-lc-rs` feature; the build fails when neither is enabled.

// SQLx is a required dependency used by the source, slot, lag, and store
// modules, so fail loudly instead of silently building it without TLS.
#[cfg(not(any(feature = "tls-rustls-ring", feature = "tls-rustls-aws-lc-rs")))]
compile_error!(
    "Either the `tls-rustls-ring` or the `tls-rustls-aws-lc-rs` feature must be enabled."
);

pub mod application_name;
pub mod default_expression;
pub mod lag;
pub mod numeric;
pub mod publications;
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
