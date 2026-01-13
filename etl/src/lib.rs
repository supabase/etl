//! <p align="center">
//!   <img src="https://raw.githubusercontent.com/supabase/supabase/master/packages/common/assets/images/supabase-logo-wordmark--light.svg" alt="Supabase" width="480">
//! </p>
//!
//! This crate provides a high-performance, streaming ETL system built on Postgres logical replication.
//!
//! # Key Features
//!
//! - **Real-time streaming**: Uses Postgres logical replication for minimal latency
//! - **Destination agnostic**: Implement your own custom destinations
//! - **Robust error handling**: Comprehensive error classification with retry strategies
//! - **Concurrent processing**: Parallel table synchronization
//! - **Suspendable**: Persistent tracking of replication progress
//! - **Read replica support**: Optional heartbeat mechanism for replica mode
//!
//! # Basic Usage Example
//!
//! ```rust,no_run
//! use etl::{
//!     config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig, TableSyncCopyConfig},
//!     destination::memory::MemoryDestination,
//!     pipeline::Pipeline,
//!     store::both::memory::MemoryStore,
//! };
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pg_config = PgConnectionConfig {
//!         host: "localhost".to_string(),
//!         port: 5432,
//!         name: "mydb".to_string(),
//!         username: "postgres".to_string(),
//!         password: Some("password".to_string().into()),
//!         tls: TlsConfig { enabled: false, trusted_root_certs: String::new() },
//!         keepalive: None
//!     };
//!
//!     let store = MemoryStore::new();
//!     let destination = MemoryDestination::new();
//!
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
//!         table_sync_copy: TableSyncCopyConfig::IncludeAllTables,
//!     };
//!
//!     let mut pipeline = Pipeline::new(config, store, destination);
//!     pipeline.start().await?;
//!     pipeline.wait().await?;
//!
//!     Ok(())
//! }
//! ```

mod concurrency;
pub mod config;
mod conversions;
pub mod destination;
#[cfg(feature = "egress")]
pub mod egress;
pub mod error;
#[cfg(feature = "failpoints")]
pub mod failpoints;
pub mod macros;
pub mod metrics;
pub mod pipeline;
pub mod replication;
pub mod state;
pub mod store;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
pub mod types;
mod utils;
pub mod workers;
