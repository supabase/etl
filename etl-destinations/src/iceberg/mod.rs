//! Apache Iceberg destination for ETL pipelines.
//!
//! This module provides a complete implementation of the ETL destination trait for Apache Iceberg,
//! enabling real-time data replication from PostgreSQL to Iceberg tables with full CDC support.
//!
//! # Features
//!
//! - **Real-time CDC**: Support for INSERT, UPDATE, DELETE, and TRUNCATE operations
//! - **Schema Evolution**: Automatic conversion from PostgreSQL to Iceberg schemas
//! - **Multiple Catalogs**: Support for REST, SQL, and Glue catalogs
//! - **Cloud Storage**: Integration with S3, GCS, Azure, and local filesystem
//! - **Batch Optimization**: Intelligent batching for optimal performance
//! - **Error Recovery**: Comprehensive retry logic with exponential backoff
//! - **Monitoring**: Built-in metrics and structured logging
//!
//! # Example Usage
//!
//! ```rust,no_run
//! use etl_destinations::iceberg::IcebergDestination;
//! use etl::store::both::memory::MemoryStore;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create destination with store
//! let store = MemoryStore::new();
//! let destination = IcebergDestination::new(
//!     "http://localhost:8181".to_string(),
//!     "s3://my-bucket/warehouse".to_string(),
//!     "etl".to_string(),
//!     None,
//!     store,
//! ).await?;
//!
//! // Use with ETL pipeline
//! // let pipeline = Pipeline::new(pg_config, destination, store);
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod config;
pub mod core;
pub mod encoding;
pub mod schema;

// Re-export main types for convenience
pub use client::IcebergClient;
pub use core::IcebergDestination;
pub use schema::{CellToArrowConverter, SchemaMapper};

/// Default namespace for Iceberg tables.
pub const DEFAULT_NAMESPACE: &str = "etl";

/// Default table prefix for PostgreSQL tables.
pub const DEFAULT_TABLE_PREFIX: &str = "pg_";

/// CDC metadata column names for consistency across ETL destinations.
pub mod cdc_columns {
    /// Column indicating the type of change (INSERT, UPDATE, DELETE, UPSERT).
    pub const CHANGE_TYPE: &str = "_CHANGE_TYPE";

    /// Column containing the sequence number for ordering events.
    pub const CHANGE_SEQUENCE_NUMBER: &str = "_CHANGE_SEQUENCE_NUMBER";

    /// Column containing the timestamp when the change occurred.
    pub const CHANGE_TIMESTAMP: &str = "_CHANGE_TIMESTAMP";
}

/// CDC operation types.
pub mod cdc_operations {
    /// Insert operation.
    pub const INSERT: &str = "INSERT";

    /// Update operation.
    pub const UPDATE: &str = "UPDATE";

    /// Delete operation.
    pub const DELETE: &str = "DELETE";

    /// Upsert operation (used for initial table sync).
    pub const UPSERT: &str = "UPSERT";
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_NAMESPACE, "etl");
        assert_eq!(DEFAULT_TABLE_PREFIX, "pg_");
        assert_eq!(cdc_columns::CHANGE_TYPE, "_CHANGE_TYPE");
        assert_eq!(cdc_operations::INSERT, "INSERT");
    }
}
