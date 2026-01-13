//! # ETL - Postgres Logical Replication Library
//!
//! A Rust library for streaming changes from PostgreSQL databases using logical replication.
//! ETL provides building blocks for creating change data capture (CDC) pipelines.
//!
//! ## Features
//!
//! - **Logical replication**: Stream row-level changes (INSERT, UPDATE, DELETE) from Postgres.
//! - **Pluggable destinations**: Implement the [`Destination`] trait to send data anywhere.
//! - **Initial sync**: Automatically copy existing table data before streaming changes.
//! - **Robust error handling**: Comprehensive error classification with retry strategies.
//! - **Concurrent processing**: Parallel table synchronization and event application for increased throughput.
//! - **Suspendable**: Persistent tracking of replication progress which allows the pipeline to be safely paused and restarted.
//! - **Read replica support**: Optional heartbeat mechanism for replicating from read replicas.
//!
//! # Core Concepts
//!
//! ## Pipeline
//!
//! The [`Pipeline`] is the main entry point. It coordinates:
//! - Connection to PostgreSQL.
//! - Schema discovery for published tables.
//! - Initial data synchronization.
//! - Continuous change streaming.
//!
//! ## Destination
//!
//! Implement [`Destination`] to define where replicated data goes:
//!
//! ```ignore
//! use etl::destination::Destination;
//! use etl::error::EtlResult;
//! use etl::types::{Event, TableRow};
//! use etl_postgres::types::TableId;
//!
//! struct MyDestination;
//!
//! impl Destination for MyDestination {
//!     fn name() -> &'static str { "my_dest" }
//!
//!     async fn write_table_rows(&self, table_id: TableId, rows: Vec<TableRow>) -> EtlResult<()> {
//!         // Write initial sync rows.
//!         Ok(())
//!     }
//!
//!     async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
//!         // Write streaming events.
//!         Ok(())
//!     }
//!
//!     async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
//!         // Handle table truncation.
//!         Ok(())
//!     }
//! }
//! ```
//!
//! ## Quick Start
//!
//! ```ignore
//! use etl::pipeline::Pipeline;
//! use etl_config::shared::{PipelineConfig, PgConnectionConfig, BatchConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = PipelineConfig {
//!         id: 1,
//!         publication_name: "my_publication".to_string(),
//!         pg_connection: pg_config,
//!         primary_connection: None,
//!         heartbeat: None,
//!         batch: BatchConfig { max_size: 1000, max_fill_ms: 5000 },
//!         table_error_retry_delay_ms: 10000,
//!         table_error_retry_max_attempts: 5,
//!         max_table_sync_workers: 4,
//!         table_sync_copy: Default::default(),
//!     };
//!
//!     let mut pipeline = Pipeline::new(config, store, destination);
//!     pipeline.start().await?;
//!     pipeline.shutdown_and_wait().await?;
//!     Ok(())
//! }
//! ```

pub mod concurrency;
pub mod destination;
pub mod error;
mod metrics;
pub mod pipeline;
mod replication;
mod schema_store;
pub mod state;
pub mod store;
#[cfg(feature = "test-utils")]
pub mod test_utils;
pub mod types;
mod utils;
mod workers;
