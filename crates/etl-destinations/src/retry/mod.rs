//! Retry classification and backoff for destination calls.
//!
//! Each destination classifies a failed call where it happens. Its client
//! error type implements [`ClassifiedError`], and [`classified_error`]
//! constructs an [`etl::error::EtlError`] carrying that classification. The
//! pipeline's retry policy and destination-owned retry loops both read that
//! answer.

#[cfg(any(feature = "bigquery", feature = "ducklake", feature = "snowflake"))]
mod backoff;
mod classify;

#[cfg(any(feature = "bigquery", feature = "ducklake"))]
pub(crate) use backoff::RetryAttempt;
#[cfg(any(feature = "bigquery", feature = "ducklake", feature = "snowflake"))]
pub(crate) use backoff::{RetryPolicy, retry_with_backoff};
#[cfg(any(feature = "bigquery", feature = "clickhouse", feature = "ducklake", test))]
pub(crate) use classify::classified_error_with_detail;
pub(crate) use classify::{ClassifiedError, classified_error};
