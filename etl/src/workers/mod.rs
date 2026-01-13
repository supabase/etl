//! Workers for handling different pipeline phases.
//!
//! Each worker handles a specific part of the replication process:
//! - **apply**: Processes events from the replication stream.
//! - **table_sync**: Handles initial table synchronization.
//! - **heartbeat**: Maintains replication slot activity for read replicas.

pub mod apply;
pub mod base;
pub mod heartbeat;
pub mod pool;
pub mod table_sync;
