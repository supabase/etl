/// Worker management for MySQL replication.
///
/// This module provides utilities for managing replication workers in the ETL pipeline.
/// Unlike PostgreSQL's logical replication workers, MySQL workers are adapted for
/// binlog-based change data capture.

use crate::types::TableId;

/// Represents a worker type in the replication pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerType {
    /// Apply worker that processes binlog events.
    Apply,
    /// Table sync worker that performs initial table synchronization.
    TableSync { table_id: TableId },
}

/// Represents the state of a replication worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerState {
    /// Worker is initializing.
    Initializing,
    /// Worker is running normally.
    Running,
    /// Worker has stopped.
    Stopped,
    /// Worker encountered an error.
    Errored { reason: String },
}

/// Configuration for a replication worker.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// The pipeline ID this worker belongs to.
    pub pipeline_id: u64,
    /// The type of worker.
    pub worker_type: WorkerType,
    /// The current state of the worker.
    pub state: WorkerState,
}

impl WorkerConfig {
    pub fn new(pipeline_id: u64, worker_type: WorkerType) -> Self {
        Self {
            pipeline_id,
            worker_type,
            state: WorkerState::Initializing,
        }
    }

    /// Updates the worker state.
    pub fn set_state(&mut self, state: WorkerState) {
        self.state = state;
    }

    /// Returns whether the worker is currently running.
    pub fn is_running(&self) -> bool {
        matches!(self.state, WorkerState::Running)
    }

    /// Returns whether the worker has encountered an error.
    pub fn is_errored(&self) -> bool {
        matches!(self.state, WorkerState::Errored { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_config() {
        let mut config = WorkerConfig::new(1, WorkerType::Apply);
        assert_eq!(config.state, WorkerState::Initializing);
        assert!(!config.is_running());

        config.set_state(WorkerState::Running);
        assert!(config.is_running());
        assert!(!config.is_errored());

        config.set_state(WorkerState::Errored {
            reason: "Test error".to_string(),
        });
        assert!(config.is_errored());
        assert!(!config.is_running());
    }

    #[test]
    fn test_worker_types() {
        let apply = WorkerType::Apply;
        let table_sync = WorkerType::TableSync {
            table_id: TableId::new(123),
        };

        assert_ne!(apply, table_sync);
    }
}
