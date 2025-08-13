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
//! ## Replication
//! The [`replication`] module handles PostgreSQL logical replication protocol details,
//! including slot management, streaming changes, and maintaining consistency.
//!
//! ## State Management
//! The [`store`] module provides persistent state tracking for replication progress,
//! table schemas, and synchronization status across restarts.
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
pub mod state;
pub mod store;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
pub mod types;
mod utils;
pub mod workers;
