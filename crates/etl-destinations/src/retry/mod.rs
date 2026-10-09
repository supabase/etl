//! Retry classification and backoff for destination calls.
//!
//! Each destination classifies a failed call once, where it happens. Its
//! client error type implements [`ClassifiedError`], and
//! [`EtlErrorExt::caused_by`] records that answer on the
//! [`etl::error::EtlError`] it attaches the error to. The pipeline's retry
//! policy and destination-owned retry loops both read that answer.

#[cfg(any(feature = "bigquery", feature = "ducklake", feature = "snowflake"))]
mod backoff;
mod classify;

#[cfg(any(feature = "bigquery", feature = "ducklake", feature = "snowflake"))]
pub(crate) use backoff::{RetryAttempt, RetryDecision, RetryPolicy, retry_with_backoff};
pub(crate) use classify::{ClassifiedError, EtlErrorExt};
