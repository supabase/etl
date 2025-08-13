//! PostgreSQL logical replication ETL library.
//!
//! This crate provides a high-performance, streaming ETL (Extract, Transform, Load) system
//! built on PostgreSQL logical replication. It enables real-time data synchronization
//! from PostgreSQL databases to various destinations with configurable transformations
//! and robust error handling.
//!
//! # Key Features
//!
//! - **Real-time streaming**: Uses PostgreSQL logical replication for minimal latency
//! - **Configurable pipelines**: Define sources, destinations, and transformation rules
//! - **Robust error handling**: Comprehensive error classification with retry strategies
//! - **Schema evolution**: Automatic handling of DDL changes in source databases
//! - **Concurrent processing**: Parallel table synchronization and event application
//! - **State management**: Persistent tracking of replication progress and table states
//! - **Type-safe conversions**: Automatic handling of PostgreSQL to destination type mappings
//!
//! # Core Concepts
//!
//! ## Pipeline
//! A [`pipeline::Pipeline`] represents a complete ETL workflow that connects a PostgreSQL publication
//! to one or more destinations. It manages the replication stream, applies transformations,
//! and handles failures gracefully.
//!
//! ## Destinations  
//! [`destination::Destination`] trait implementations define where replicated data should be sent.
//! Built-in destinations include in-memory storage for testing and external integrations.
//!
//! ## Replication
//! The [`replication`] module handles PostgreSQL logical replication protocol details,
//! including slot management, streaming changes, and maintaining consistency.
//!
//! ## State Management
//! The [`store`] module provides persistent state tracking for replication progress,
//! table schemas, and synchronization status across restarts.
//!
//! # Basic Usage
//!
//! ```rust,no_run
//! use etl::prelude::*;
//! use etl::destination::memory::MemoryDestination;
//! use etl::store::both::memory::MemoryStore;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Configure connection to source PostgreSQL database
//! let pg_config = PgConnectionConfig {
//!     host: "localhost".to_string(),
//!     port: 5432,
//!     name: "source_db".to_string(),
//!     username: "postgres".to_string(),
//!     password: None,
//!     tls: TlsConfig {
//!         enabled: false,
//!         trusted_root_certs: String::new(),
//!     },
//! };
//!
//! // Create pipeline configuration
//! let pipeline_config = PipelineConfig {
//!     id: 1,
//!     pg_connection: pg_config,
//!     publication_name: "my_publication".to_string(),
//!     batch: BatchConfig::default(),
//!     table_error_retry_delay_ms: 5000,
//!     max_table_sync_workers: 4,
//! };
//!
//! // Set up state store and destination
//! let store = MemoryStore::new();  
//! let destination = MemoryDestination::new();
//!
//! // Create and start pipeline
//! let mut pipeline = Pipeline::new(
//!     1, // PipelineId is just u64
//!     pipeline_config,
//!     store,
//!     destination
//! );
//!
//! pipeline.start().await?;
//! pipeline.wait().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Advanced Features
//!
//! ## Custom Destinations
//! Implement the [`destination::Destination`] trait to send replicated data to custom systems:
//!
//! ```rust,no_run  
//! use etl::prelude::*;
//!
//! struct CustomDestination {
//!     // destination-specific state
//! }
//!
//! impl Destination for CustomDestination {
//!     async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
//!         // Implement table truncation logic for your system
//!         println!("Truncating table {:?}", table_id);
//!         Ok(())
//!     }
//!
//!     async fn write_table_rows(&self, table_id: TableId, rows: Vec<TableRow>) -> EtlResult<()> {
//!         // Implement batch row writing logic for your system
//!         println!("Writing {} rows to table {:?}", rows.len(), table_id);
//!         Ok(())  
//!     }
//!
//!     async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
//!         // Implement streaming event processing logic for your system
//!         println!("Processing {} events", events.len());
//!         Ok(())
//!     }
//! }
//! ```
//!
//! ## Error Handling
//! All operations return [`error::EtlResult<T>`] which provides detailed error classification
//! for implementing appropriate retry and recovery strategies.
//!
//! # Feature Flags
//!
//! - `unknown-types-to-bytes`: Convert unknown PostgreSQL types to byte arrays (default)
//! - `test-utils`: Enable testing utilities and mock implementations  
//! - `failpoints`: Enable fault injection for testing error scenarios

mod concurrency;
pub mod config;
mod conversions;
pub mod destination;
pub mod error;
#[cfg(feature = "failpoints")]
pub mod failpoints;
pub mod macros;
pub mod pipeline;
pub mod replication;
pub mod schema;
pub mod state;
pub mod store;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
pub mod types;
mod utils;
pub mod workers;

/// Commonly used types and traits.
///
/// This module re-exports the most frequently used items from the crate,
/// making them available with a simple `use etl::prelude::*;` import.
/// It includes configuration types, error handling, and essential traits
/// needed for most ETL pipeline implementations.
pub mod prelude {
    pub use crate::destination::Destination;
    pub use crate::error::{ErrorKind, EtlError, EtlResult};
    pub use crate::pipeline::Pipeline;
    pub use crate::types::{Event, PipelineId, TableRow};
    pub use crate::{bail, etl_error};
    pub use etl_config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
    pub use etl_postgres::schema::TableId;
}
