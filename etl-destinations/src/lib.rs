//! ETL destination implementations.
//!
//! Provides implementations of the ETL [`Destination`] trait for various data warehouses
//! and analytics platforms, enabling data replication from PostgreSQL to cloud services.

#[cfg(feature = "bigquery")]
pub mod bigquery;
