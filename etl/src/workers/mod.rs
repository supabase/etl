//! Background workers for ETL processing.
//!
//! This module provides the worker architecture for parallel ETL operations.
//! Workers handle different aspects of the replication process, from applying
//! streaming changes to synchronizing initial table data.
//!
//! Key worker types:
//! - [`apply::ApplyWorker`] - Processes replication stream events
//! - [`table_sync::TableSyncWorker`] - Synchronizes individual table data
//! - [`pool::TableSyncWorkerPool`] - Manages multiple table sync workers

pub mod apply;
pub mod base;
pub mod pool;
pub mod table_sync;
