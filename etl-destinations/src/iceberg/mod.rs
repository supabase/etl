//! Apache Iceberg destination implementation for ETL pipelines.
//!
//! This module provides a complete implementation for streaming PostgreSQL data
//! to Apache Iceberg tables using the REST catalog interface. It handles real-time
//! change data capture (CDC), schema evolution, and efficient batch processing.
//!
//! # Features
//!
//! - **Real-time CDC**: Support for INSERT, UPDATE, DELETE, and TRUNCATE operations
//! - **Schema Evolution**: Automatic schema mapping from PostgreSQL to Iceberg types
//! - **Efficient Batching**: Optimized for cloud storage with configurable batch sizes
//! - **REST Catalog**: Full integration with Iceberg REST catalog specification
//! - **Arrow Integration**: High-performance data serialization using Apache Arrow
//!
//! # Architecture
//!
//! The implementation is organized into several specialized modules:
//!
//! - `config`: Configuration structures for writer behavior and performance tuning
//! - `schema`: PostgreSQL to Iceberg/Arrow schema mapping and type conversion
//! - `encoding`: Data encoding and Arrow RecordBatch creation
//! - `client`: REST catalog client for Iceberg table operations
//! - `core`: Main destination implementation with CDC support
//!
//! # Usage
//!
//! ```no_run
//! use etl_destinations::iceberg::IcebergDestination;
//! use etl::destination::Destination;
//!
//! # tokio_test::block_on(async {
//! let destination = IcebergDestination::new(
//!     "http://localhost:8181".to_string(),
//!     "s3://warehouse/".to_string(),
//!     "my_namespace".to_string(),
//!     None, // Optional auth token
//!     store,
//! ).await?;
//!
//! // Use as ETL destination
//! destination.write_events(events).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # }).unwrap();
//! ```

// Module structure will be expanded in subsequent PRs
