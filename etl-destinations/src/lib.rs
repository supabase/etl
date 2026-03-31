//! ETL destination implementations.
//!
//! Provides implementations of the ETL destination trait for various data warehouses
//! and analytics platforms, enabling data replication from Postgres to cloud services.

#[cfg(any(feature = "bigquery", feature = "ducklake", feature = "iceberg"))]
pub(crate) mod retry;
mod table_name;

#[cfg(feature = "bigquery")]
pub mod bigquery;
#[cfg(feature = "ducklake")]
pub mod ducklake;
#[cfg(feature = "egress")]
pub mod egress;
#[cfg(feature = "iceberg")]
pub mod iceberg;
