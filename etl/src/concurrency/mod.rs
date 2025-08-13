//! Concurrency utilities for coordinating ETL pipeline operations.
//!
//! This module provides the fundamental concurrency primitives used throughout the ETL system
//! to coordinate multiple workers, handle graceful shutdown, and manage streaming data flows.
//! The design emphasizes safety, predictable cleanup, and deadlock-free operation.
//!
//! # Architecture Overview
//!
//! The ETL system uses a multi-worker architecture where different types of workers coordinate
//! through well-defined concurrency patterns:
//!
//! - **Apply workers** process the main replication stream
//! - **Table sync workers** handle initial data copying for individual tables
//! - **Worker pools** manage lifecycle of multiple table sync workers
//! - **Pipeline coordinators** orchestrate the overall ETL process
//!
//! # Coordination Patterns
//!
//! ## Graceful Shutdown
//!
//! The [`shutdown`] module implements a broadcast-based shutdown pattern where:
//! 1. A single shutdown signal can terminate multiple workers simultaneously
//! 2. Workers complete their current operations before terminating
//! 3. The shutdown process respects transaction boundaries and data consistency
//! 4. Resource cleanup happens in the correct order to prevent deadlocks
//!
//! ## Worker Coordination
//!
//! The [`signal`] module provides lightweight signaling mechanisms for:
//! - Notifying apply workers when table sync operations complete
//! - Triggering state transitions between different replication phases
//! - Coordinating batch processing and flow control
//!
//! ## Stream Processing
//!
//! The [`stream`] module implements streaming patterns that:
//! - Handle backpressure from slow destinations
//! - Batch events for efficient processing while respecting transaction boundaries
//! - Integrate shutdown signals into stream processing pipelines
//! - Provide timeout-based batching to prevent indefinite delays
//!
//! # Safety Guarantees
//!
//! All concurrency utilities in this module are designed to:
//! - **Avoid deadlocks**: No circular waiting dependencies between workers
//! - **Prevent data loss**: Shutdown only occurs at safe transaction boundaries
//! - **Maintain consistency**: State transitions are atomic and properly ordered
//! - **Handle failures gracefully**: Partial failures don't cascade to other workers
//!
//! # Usage Patterns
//!
//! The concurrency utilities are primarily used internally by the ETL system
//! and are not part of the public API. They provide the foundation for
//! coordinating worker processes, handling graceful shutdown, and managing
//! streaming data flows throughout the pipeline.

pub mod shutdown;
pub mod signal;
pub mod stream;
