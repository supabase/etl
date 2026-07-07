//! <p align="center">
//!   <img src="https://raw.githubusercontent.com/supabase/supabase/master/packages/common/assets/images/supabase-logo-wordmark--light.svg" alt="Supabase" width="480">
//! </p>
//!
//! This crate provides a high-performance, streaming ETL (Extract, Transform,
//! Load) system built on Postgres logical replication. It enables real-time
//! data synchronization from Postgres databases to destination systems with a
//! typed Rust API and robust error handling.
//!
//! # Key Features
//!
//! - **Real-time streaming**: Uses Postgres logical replication for minimal
//!   latency
//! - **Destination agnostic**: Implement your own custom destinations to which
//!   data will be sent
//! - **Robust error handling**: Comprehensive error classification with retry
//!   strategies
//! - **Concurrent processing**: Parallel table synchronization and event
//!   application for increased throughput
//! - **Suspendable**: Persistent tracking of replication progress which allows
//!   the pipeline to be safely paused and restarted
//!
//! # Core Concepts
//!
//! ## Pipeline
//! A [`pipeline::Pipeline`] represents a complete ETL workflow that connects a
//! Postgres publication to a destination. It manages the replication stream,
//! coordinates initial copy and streaming workers, and handles failures
//! gracefully.
//!
//! ## Destinations
//! [`destination::Destination`] trait implementations define where replicated
//! data should be sent. Destinations are pluggable and can integrate with
//! external systems.
//!
//! ## Store
//! The [`store::PipelineStore`] trait defines the complete store surface needed
//! by the pipeline runtime. It combines narrower capabilities for table
//! schemas, table state, destination table metadata, and table lifecycle
//! operations. These stores are critical to a pipeline's operation, as they
//! allow it to be safely paused and resumed.
//!
//! The [`store::StateStore`] trait handles table states,
//! durable replication progress, and destination table metadata, providing a
//! single interface for all state-related storage operations.
//!
//! The [`store::SchemaStore`] trait handles versioned table schemas,
//! and [`store::TableStateLifecycleStore`] handles table-scoped preparation,
//! reset, and deletion operations that must update state, schema, and metadata
//! consistently.
//! [`store::SharedStateStore`], [`store::DestinationStore`], and
//! [`store::PipelineStore`] are facade traits for code that needs common
//! combinations of these capabilities.
//!
//! **Note:** To pause and resume a pipeline after the process is stopped, it
//! must be able to persist data durably. The crate itself provides no
//! durability guarantees as it only transfers data between Postgres and the
//! destination relying on the store traits to provide the required
//! data when needed.
//!
//! ## Error Handling
//! All operations return [`error::EtlResult<T>`] which provides detailed error
//! classification for implementing appropriate retry and recovery strategies.
//!
//! # Basic Usage Example
//!
//! ```rust,no_run
//! use etl::{
//!     config::{
//!         BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
//!         PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
//!     },
//!     data::TableRow,
//!     destination::{
//!         Destination, DestinationWriteStatus, DropTableForCopyResult, WriteEventsResult,
//!         WriteTableRowsResult,
//!     },
//!     error::EtlResult,
//!     event::Event,
//!     pipeline::Pipeline,
//!     schema::ReplicatedTableSchema,
//!     store::MemoryStore,
//! };
//!
//! #[derive(Clone)]
//! struct NoopDestination;
//!
//! impl Destination for NoopDestination {
//!     fn name() -> &'static str {
//!         "noop"
//!     }
//!     async fn drop_table_for_copy(
//!         &self,
//!         _replicated_table_schema: &ReplicatedTableSchema,
//!         async_result: DropTableForCopyResult<()>,
//!     ) -> EtlResult<()> {
//!         async_result.send(Ok(()));
//!         Ok(())
//!     }
//!     async fn write_table_rows(
//!         &self,
//!         _replicated_table_schema: &ReplicatedTableSchema,
//!         _table_rows: Vec<TableRow>,
//!         async_result: WriteTableRowsResult<()>,
//!     ) -> EtlResult<()> {
//!         async_result.send(Ok(()));
//!         Ok(())
//!     }
//!     async fn write_events(
//!         &self,
//!         _events: Vec<Event>,
//!         async_result: WriteEventsResult,
//!     ) -> EtlResult<()> {
//!         async_result.send(Ok(DestinationWriteStatus::Durable));
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Configure Postgres connection
//!     let pg_config = PgConnectionConfig {
//!         host: "localhost".to_string(),
//!         hostaddr: None,
//!         port: 5432,
//!         name: "mydb".to_string(),
//!         username: "postgres".to_string(),
//!         password: Some("password".to_string().into()),
//!         tls: TlsConfig { enabled: false, trusted_root_certs: String::new() },
//!         keepalive: TcpKeepaliveConfig::default(),
//!     };
//!
//!     // Create memory-based store and destination for testing.
//!     let store = MemoryStore::new();
//!     let destination = NoopDestination;
//!
//!     // Configure the pipeline
//!     let config = PipelineConfig {
//!         id: 1,
//!         publication_name: "my_publication".to_string(),
//!         pg_connection: pg_config,
//!         store_pg_connection: None,
//!         batch: BatchConfig {
//!             max_fill_ms: 5000,
//!             memory_budget_ratio: 0.2,
//!             max_bytes: 8 * 1024 * 1024,
//!         },
//!         table_error_retry_delay_ms: 10_000,
//!         table_error_retry_max_attempts: 5,
//!         max_table_sync_workers: 4,
//!         max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
//!         memory_refresh_interval_ms: 100,
//!         replication_lag_refresh_interval_ms: 10_000,
//!         memory_backpressure: Some(MemoryBackpressureConfig::default()),
//!         table_sync_copy: TableSyncCopyConfig::default(),
//!         invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
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
//! - `egress`: Enable structured egress logging helpers
//! - `test-utils`: Enable testing utilities and mock implementations
//! - `failpoints`: Enable fault injection for testing error scenarios

pub mod config;
pub mod data;
pub mod destination;
#[cfg(feature = "egress")]
mod egress;
pub mod error;
pub mod event;
#[doc(hidden)]
#[cfg(feature = "failpoints")]
pub mod failpoints;
#[doc(hidden)]
#[cfg(feature = "fuzzing")]
pub mod fuzzing;
mod macros;
mod observability;
pub mod pipeline;
#[doc(hidden)]
#[cfg(any(test, feature = "test-utils"))]
pub mod postgres;
#[cfg(not(any(test, feature = "test-utils")))]
mod postgres;
mod replication;
mod runtime;
pub mod schema;
pub mod store;
#[doc(hidden)]
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
