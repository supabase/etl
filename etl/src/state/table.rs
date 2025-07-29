use chrono::{DateTime, Duration, Utc};
use postgres::schema::TableId;
use std::fmt;
use tokio_postgres::types::PgLsn;

use crate::error::{ErrorKind, EtlError};

/// Standard retry intervals for different types of transient errors.
mod retry_intervals {

    use chrono::Duration;

    /// Standard retry interval for connection and destination service issues.
    pub const CONNECTION_ERROR: Duration = Duration::minutes(1);

    /// Longer retry interval for authentication issues that may need token refresh.
    pub const AUTHENTICATION_ERROR: Duration = Duration::minutes(2);
}

/// Represents an error that occurred during table replication.
///
/// Contains diagnostic information including the table that failed, the reason for failure,
/// an optional solution suggestion, and the retry policy to apply.
#[derive(Debug)]
#[allow(dead_code)]
pub struct TableReplicationError {
    table_id: TableId,
    reason: String,
    solution: Option<String>,
    retry_policy: RetryPolicy,
}

impl TableReplicationError {
    /// Creates a new [`TableReplicationError`] with a suggested solution.
    pub fn with_solution(
        table_id: TableId,
        reason: impl ToString,
        solution: impl ToString,
        retry_policy: RetryPolicy,
    ) -> Self {
        Self {
            table_id,
            reason: reason.to_string(),
            solution: Some(solution.to_string()),
            retry_policy,
        }
    }

    /// Creates a new [`TableReplicationError`] without a suggested solution.
    pub fn without_solution(
        table_id: TableId,
        reason: impl ToString,
        retry_policy: RetryPolicy,
    ) -> Self {
        Self {
            table_id,
            reason: reason.to_string(),
            solution: None,
            retry_policy,
        }
    }

    /// Returns the [`TableId`] of the table that failed replication.
    pub fn table_id(&self) -> TableId {
        self.table_id
    }
}

/// Converts an [`EtlError`] into a [`TableReplicationError`].
///
/// Currently, uses placeholder values for the table ID and provides a generic solution.
impl TableReplicationError {
    /// Converts an [`EtlError`] to a [`TableReplicationError`] for a specific table.
    ///
    /// Determines appropriate retry policies based on the error kind.
    ///
    /// Note that this conversion is constantly improving since during testing and operation of ETL
    /// we might notice edge cases that could be manually handled.
    pub fn from_etl_error(table_id: TableId, error: EtlError) -> Self {
        match error.kind() {
            // Transient errors with retry
            ErrorKind::ConnectionFailed => Self::with_solution(
                table_id,
                error,
                "Check network connectivity and database availability",
                RetryPolicy::retry_in(retry_intervals::CONNECTION_ERROR),
            ),
            ErrorKind::AuthenticationError => Self::with_solution(
                table_id,
                error,
                "Check credentials and token validity",
                RetryPolicy::retry_in(retry_intervals::AUTHENTICATION_ERROR),
            ),

            // Errors that could disappear after user intervention
            ErrorKind::SourceSchemaError => Self::with_solution(
                table_id,
                error,
                "Fix the schema of the Postgres database",
                RetryPolicy::UserIntervention,
            ),
            ErrorKind::ConfigError => Self::with_solution(
                table_id,
                error,
                "Fix application or service configuration",
                RetryPolicy::UserIntervention,
            ),
            ErrorKind::ReplicationSlotAlreadyExists => Self::with_solution(
                table_id,
                error,
                "Remove the existing slot from the Postgres database",
                RetryPolicy::UserIntervention,
            ),
            ErrorKind::ReplicationSlotNotCreated => Self::with_solution(
                table_id,
                error,
                "Check if the Postgres database allows the creation of new replication slots",
                RetryPolicy::UserIntervention,
            ),

            // By default, all errors are not retriable
            _ => Self::without_solution(table_id, error, RetryPolicy::None),
        }
    }
}

/// Defines the retry strategy for a failed table replication.
#[derive(Debug)]
pub enum RetryPolicy {
    /// No retry should be attempted.
    None,
    /// Retry requires user intervention before proceeding.
    UserIntervention,
    /// Retry after the specified timestamp.
    Retry { next_retry: DateTime<Utc> },
}

impl RetryPolicy {

    pub fn retry_in(duration: Duration) -> Self {
        Self::Retry { next_retry: Utc::now() + duration }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TableReplicationPhase {
    /// Set by the pipeline when it first starts and encounters a table for the first time
    Init,

    /// Set by table-sync worker just before starting initial table copy
    DataSync,

    /// Set by table-sync worker when initial table copy is done
    FinishedCopy,

    /// Set by table-sync worker when waiting for the apply worker to pause
    /// On every transaction boundary the apply worker checks if any table-sync
    /// worker is in the SyncWait state and pauses itself if it finds any.
    /// This phase is stored in memory only and not persisted to the state store
    SyncWait,

    /// Set by the apply worker when it is paused. The table-sync worker waits
    /// for the apply worker to set this state after setting the state to SyncWait.
    /// This phase is stored in memory only and not persisted to the state store
    Catchup {
        /// The lsn to catch up to. This is the location where the apply worker is paused
        lsn: PgLsn,
    },

    /// Set by the table-sync worker when catch-up phase is completed and table-sync
    /// worker has caught up with the apply worker's lsn position
    SyncDone {
        /// The lsn up to which the table-sync worker has caught up
        lsn: PgLsn,
    },

    /// Set by apply worker when it has caught up with the table-sync worker's
    /// catch up lsn position. Tables with this state have successfully run their
    /// initial table copy and catch-up phases and any changes to them will now
    /// be applied by the apply worker only
    Ready,

    /// Set by either the table-sync worker or the apply worker when a table is no
    /// longer being synced because of an error. Tables in this state can only
    /// start syncing again after a manual intervention from the user.
    // TODO: turn this into a generic `Error` state with more information.
    Skipped,
}

impl TableReplicationPhase {
    pub fn as_type(&self) -> TableReplicationPhaseType {
        self.into()
    }
}

impl From<TableReplicationError> for TableReplicationPhase {
    fn from(_value: TableReplicationError) -> Self {
        // TODO: implement actual conversion with proper values once `Skipped` is converted to `Errored`
        //  and the fields are added.
        Self::Skipped
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TableReplicationPhaseType {
    Init,
    DataSync,
    FinishedCopy,
    SyncWait,
    Catchup,
    SyncDone,
    Ready,
    Skipped,
}

impl TableReplicationPhaseType {
    /// Returns `true` if the phase should be saved into the state store, `false` otherwise.
    pub fn should_store(&self) -> bool {
        match self {
            Self::Init => true,
            Self::DataSync => true,
            Self::FinishedCopy => true,
            Self::SyncWait => false,
            Self::Catchup => false,
            Self::SyncDone => true,
            Self::Ready => true,
            Self::Skipped => true,
        }
    }

    /// Returns `true` if a table with this phase is done processing, `false` otherwise.
    ///
    /// A table is done processing, when its events are being processed by the apply worker instead
    /// of a table sync worker.
    pub fn is_done(&self) -> bool {
        match self {
            Self::Init => false,
            Self::DataSync => false,
            Self::FinishedCopy => false,
            Self::SyncWait => false,
            Self::Catchup => false,
            Self::SyncDone => false,
            Self::Ready => true,
            Self::Skipped => true,
        }
    }
}

impl<'a> From<&'a TableReplicationPhase> for TableReplicationPhaseType {
    fn from(phase: &'a TableReplicationPhase) -> Self {
        match phase {
            TableReplicationPhase::Init => Self::Init,
            TableReplicationPhase::DataSync => Self::DataSync,
            TableReplicationPhase::FinishedCopy => Self::FinishedCopy,
            TableReplicationPhase::SyncWait => Self::SyncWait,
            TableReplicationPhase::Catchup { .. } => Self::Catchup,
            TableReplicationPhase::SyncDone { .. } => Self::SyncDone,
            TableReplicationPhase::Ready => Self::Ready,
            TableReplicationPhase::Skipped => Self::Skipped,
        }
    }
}

impl fmt::Display for TableReplicationPhaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Init => write!(f, "init"),
            Self::DataSync => write!(f, "data_sync"),
            Self::FinishedCopy => write!(f, "finished_copy"),
            Self::SyncWait => write!(f, "sync_wait"),
            Self::Catchup => write!(f, "catchup"),
            Self::SyncDone => write!(f, "sync_done"),
            Self::Ready => write!(f, "ready"),
            Self::Skipped => write!(f, "skipped"),
        }
    }
}
