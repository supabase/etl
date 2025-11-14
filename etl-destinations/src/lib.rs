//! ETL destination implementations.
//!
//! Provides implementations of the ETL destination trait for various data warehouses
//! and analytics platforms, enabling data replication from Postgres to cloud services.

#[cfg(feature = "bigquery")]
pub mod bigquery;
pub mod encryption;
#[cfg(feature = "iceberg")]
pub mod iceberg;
#[cfg(feature = "redis")]
pub mod redis;
