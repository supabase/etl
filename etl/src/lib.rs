//! <p align="center">
//!   <img src="https://raw.githubusercontent.com/supabase/supabase/master/packages/common/assets/images/supabase-logo-wordmark--light.svg" alt="Supabase" width="480">
//! </p>
//!
//! ⚠️ **Warning:** These docs are a work in progress, for this reason they may be incomplete.
//!
//! This crate provides a high-performance, streaming ETL (Extract, Transform, Load) system
//! built on PostgreSQL logical replication. It enables real-time data synchronization
//! from PostgreSQL databases to various destinations with configurable transformations
//! and robust error handling.
//!
//! # Key Features
//!
//! - **Real-time streaming**: Uses PostgreSQL logical replication for minimal latency
//! - **Destination agnostic**: Implement your own custom destinations to which data will be sent
//! - **Robust error handling**: Comprehensive error classification with retry strategies
//! - **Concurrent processing**: Parallel table synchronization and event application for increased throughput
//! - **Suspendable**: Persistent tracking of replication progress which allows the pipeline to be safely paused and restarted
//!
//! # Core Concepts
//!
//! ## Pipeline
//! A [`pipeline::Pipeline`] represents a complete ETL workflow that connects a PostgreSQL publication
//! to a destination. It manages the replication stream, applies transformations,
//! and handles failures gracefully.
//!
//! ## Destinations  
//! [`destination::Destination`] trait implementations define where replicated data should be sent.
//! Built-in destinations include in-memory storage for testing and external integrations.
//!
//! ## Store
//! The [`store::schema::SchemaStore`] and [`store::state::StateStore`] traits define where the
//! table schemas, replication state, and table mappings are stored. These stores are critical to a pipeline's
//! operation, as they allow it to be safely paused and resumed.
//!
//! The [`store::state::StateStore`] trait handles both table replication states and table mappings,
//! providing a single interface for all state-related storage operations.
//!
//! **Note:** To pause and resume a pipeline after the process is stopped, it must be able to
//! persist data durably. The crate itself provides no durability guarantees as it only transfers
//! data between PostgreSQL and the destination relying on the store traits to provide the required
//! data when needed.
//!
//! ## Error Handling
//! All operations return [`error::EtlResult<T>`] which provides detailed error classification
//! for implementing appropriate retry and recovery strategies.
//!
//! # Basic Usage Example
//!
//! ```rust,no_run
//! use etl::{
//!     config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig},
//!     destination::memory::MemoryDestination,
//!     pipeline::Pipeline,
//!     store::both::memory::MemoryStore,
//! };
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Configure PostgreSQL connection
//!     let pg_config = PgConnectionConfig {
//!         host: "localhost".to_string(),
//!         port: 5432,
//!         name: "mydb".to_string(),
//!         username: "postgres".to_string(),
//!         password: Some("password".to_string().into()),
//!         tls: TlsConfig { enabled: false, trusted_root_certs: String::new() },
//!     };
//!
//!     // Create memory-based store and destination for testing
//!     let store = MemoryStore::new();
//!     let destination = MemoryDestination::new();
//!
//!     // Configure the pipeline
//!     let config = PipelineConfig {
//!         id: 1,
//!         publication_name: "my_publication".to_string(),
//!         pg_connection: pg_config,
//!         batch: BatchConfig { max_size: 1000, max_fill_ms: 5000 },
//!         table_error_retry_delay_ms: 10000,
//!         max_table_sync_workers: 4,
//!     };
//!
//!     // Create and start the pipeline
//!     let mut pipeline = Pipeline::new(config, store, destination);
//!     pipeline.start().await?;
//!     
//!     // Pipeline will run until stopped
//!     pipeline.wait().await?;
//!
//!     Ok(())
//! }
//! ```
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
pub mod state;
pub mod store;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
pub mod types;
mod utils;
pub mod workers;
