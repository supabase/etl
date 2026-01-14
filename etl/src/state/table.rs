use chrono::{DateTime, Duration, Utc};
use etl_config::shared::PipelineConfig;
use etl_postgres::replication::state;
use etl_postgres::types::TableId;
use std::fmt;
use tokio_postgres::types::PgLsn;

use crate::error::{ErrorKind, EtlError};
use crate::{bail, etl_error};

/// Represents an error that occurred during table replication.
///
/// Contains diagnostic information including the table that failed, the reason for failure,
/// an optional solution suggestion, and the retry policy to apply.
#[derive(Debug)]
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

    /// Returns the retry policy for this error.
    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry_policy
    }

    /// Returns a copy of the error with the provided retry policy.
    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    /// Converts an [`EtlError`] to a [`TableReplicationError`] for a specific table.
    ///
    /// Determines appropriate retry policies based on the error kind.
    ///
    /// Note that this conversion is constantly improving since during testing and operation of ETL
    /// we might notice edge cases that could be manually handled.
    pub fn from_etl_error(config: &PipelineConfig, table_id: TableId, error: &EtlError) -> Self {
        let retry_duration = Duration::milliseconds(config.table_error_retry_delay_ms as i64);
        match error.kind() {
            // Errors that can be retried automatically
            ErrorKind::SourceConnectionFailed => {
                Self::without_solution(table_id, error, RetryPolicy::retry_in(retry_duration))
            }
            ErrorKind::DestinationConnectionFailed => {
                Self::without_solution(table_id, error, RetryPolicy::retry_in(retry_duration))
            }
            ErrorKind::SourceOperationCanceled => {
                Self::without_solution(table_id, error, RetryPolicy::retry_in(retry_duration))
            }
            ErrorKind::SourceDatabaseShutdown => {
                Self::without_solution(table_id, error, RetryPolicy::retry_in(retry_duration))
            }
            ErrorKind::SourceLockTimeout => {
                Self::without_solution(table_id, error, RetryPolicy::retry_in(retry_duration))
            }
            ErrorKind::SourceDatabaseInRecovery => {
                Self::without_solution(table_id, error, RetryPolicy::retry_in(retry_duration))
            }

            // Errors with manual retry and explicit solution
            ErrorKind::AuthenticationError => Self::with_solution(
                table_id,
                error,
                "Verify database credentials and authentication token validity.",
                RetryPolicy::ManualRetry,
            ),
            ErrorKind::SourceSchemaError => Self::with_solution(
                table_id,
                error,
                "Update the Postgres database schema to resolve compatibility issues.",
                RetryPolicy::ManualRetry,
            ),
            ErrorKind::ConfigError => Self::with_solution(
                table_id,
                error,
                "Update the application or service configuration settings.",
                RetryPolicy::ManualRetry,
            ),
            ErrorKind::ReplicationSlotAlreadyExists => Self::with_solution(
                table_id,
                error,
                "Remove the existing replication slot from the Postgres database.",
                RetryPolicy::ManualRetry,
            ),
            ErrorKind::ReplicationSlotNotCreated => Self::with_solution(
                table_id,
                error,
                "Verify the Postgres database allows creation of new replication slots.",
                RetryPolicy::ManualRetry,
            ),
            ErrorKind::SourceConfigurationLimitExceeded => Self::with_solution(
                table_id,
                error,
                "Verify the configured limits for Postgres, for example, the maximum number of replication slots.",
                RetryPolicy::ManualRetry,
            ),
            ErrorKind::NullValuesNotSupportedInArrayInDestination => Self::with_solution(
                table_id,
                error,
                "Remove NULL values from array columns in the Postgres tables.",
                RetryPolicy::ManualRetry,
            ),
            ErrorKind::UnsupportedValueInDestination => Self::with_solution(
                table_id,
                error,
                "Update the value in the Postgres table.",
                RetryPolicy::ManualRetry,
            ),
            ErrorKind::SourceSnapshotTooOld => Self::with_solution(
                table_id,
                error,
                "Check replication slot status and database configuration.",
                RetryPolicy::ManualRetry,
            ),

            // Special handling for error kinds used during failure injection.
            #[cfg(feature = "failpoints")]
            ErrorKind::WithNoRetry => Self::with_solution(
                table_id,
                error,
                "Cannot retry this error.",
                RetryPolicy::NoRetry,
            ),
            #[cfg(feature = "failpoints")]
            ErrorKind::WithManualRetry => Self::with_solution(
                table_id,
                error,
                "Manually trigger retry after resolving the issue.",
                RetryPolicy::ManualRetry,
            ),
            #[cfg(feature = "failpoints")]
            ErrorKind::WithTimedRetry => Self::with_solution(
                table_id,
                error,
                "Will automatically retry after the configured delay.",
                RetryPolicy::retry_in(retry_duration),
            ),

            // By default, all errors are retriable but without a solution. The reason for why we do
            // this is to let customers fix the system on their own, since right now we don't have
            // a clean understanding of all possible recoverable cases of the system (since we can't
            // infer that only by looking at the statically defined errors).
            _ => Self::with_solution(
                table_id,
                error,
                "There is no explicit solution for this error, if the issue persists after rollback, please contact support.",
                RetryPolicy::ManualRetry,
            ),
        }
    }
}

/// Defines the retry strategy for a failed table replication.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RetryPolicy {
    /// No retry should be attempted, the system has to be fixed by hand.
    NoRetry,
    /// Retry after it was manually triggered.
    ManualRetry,
    /// Retry after the specified timestamp.
    TimedRetry { next_retry: DateTime<Utc> },
}

impl RetryPolicy {
    pub fn retry_in(duration: Duration) -> Self {
        Self::TimedRetry {
            next_retry: Utc::now() + duration,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TableReplicationPhase {
    /// Set by the pipeline when it first starts and encounters a table for the first time.
    Init,
    /// Set by table-sync worker just before starting initial table copy.
    DataSync,
    /// Set by table-sync worker when initial table copy is done.
    FinishedCopy,
    /// Set by table-sync worker when waiting for the apply worker to pause.
    ///
    /// On every transaction boundary the apply worker checks if any table-sync
    /// worker is in the `SyncWait` state and pauses itself if it finds any. It resumes
    /// only when the table sync worker has caught up with the `Catchup`'s LSN.
    ///
    /// This phase is stored in memory only and not persisted to the state store.
    SyncWait {
        /// The LSN of the snapshot used for the initial table copy.
        ///
        /// This LSN represents the consistent point from which the table sync worker
        /// will start streaming changes. The apply worker will use `max(this lsn, current_lsn)`
        /// when setting the Catchup LSN to ensure no data loss, following PostgreSQL's pattern:
        /// `syncworker->relstate_lsn = Max(syncworker->relstate_lsn, current_lsn)`.
        lsn: PgLsn,
    },
    /// Set by the apply worker when it is paused. The table-sync worker waits
    /// for the apply worker to set this state after setting the state to `SyncWait`.
    ///
    /// This phase is stored in memory only and not persisted to the state store
    Catchup {
        /// The lsn to catch up to. This is the location where the apply worker is paused.
        lsn: PgLsn,
    },

    /// Set by the table-sync worker when catch-up phase is completed and table-sync
    /// worker has caught up with the apply worker's lsn position.
    ///
    /// The apply worker is waiting on this phase to be reached before continuing to process other
    /// tables or events in the apply loop.
    SyncDone {
        /// The lsn up to which the table-sync worker has caught up.
        ///
        /// This LSN is guaranteed to be >= `Catchup.lsn`.
        lsn: PgLsn,
    },
    /// Set by apply worker when it has caught up with the table-sync worker's
    /// catch up lsn position. Tables with this state have successfully run their
    /// initial table copy and catch-up phases and any changes to them will now
    /// be applied by the apply worker only.
    Ready,
    /// Set by either the table-sync worker or the apply worker when a table encounters
    /// an error during replication. Contains diagnostic information and retry policy.
    Errored {
        /// Human-readable description of what went wrong.
        reason: String,
        /// Optional suggestion for how to fix the issue.
        solution: Option<String>,
        /// Retry policy specifying how/when to retry.
        retry_policy: RetryPolicy,
    },
}

impl TableReplicationPhase {
    pub fn as_type(&self) -> TableReplicationPhaseType {
        self.into()
    }
}

impl fmt::Display for TableReplicationPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Init => write!(f, "init"),
            Self::DataSync => write!(f, "data_sync"),
            Self::FinishedCopy => write!(f, "finished_copy"),
            Self::SyncWait { lsn } => write!(f, "sync_wait({lsn})"),
            Self::Catchup { lsn } => write!(f, "catchup({lsn})"),
            Self::SyncDone { lsn } => write!(f, "sync_done({lsn})"),
            Self::Ready => write!(f, "ready"),
            Self::Errored { reason, .. } => write!(f, "errored({reason})"),
        }
    }
}

impl From<TableReplicationError> for TableReplicationPhase {
    fn from(value: TableReplicationError) -> Self {
        Self::Errored {
            reason: value.reason,
            solution: value.solution,
            retry_policy: value.retry_policy,
        }
    }
}

/// Converts Postgres state rows back to ETL table replication phases.
///
/// This conversion transforms persisted database state into internal ETL
/// replication phase representations. It deserializes metadata from the
/// database row and maps database state enums to ETL phase enums.
impl TryFrom<state::TableReplicationStateRow> for TableReplicationPhase {
    type Error = EtlError;

    fn try_from(value: state::TableReplicationStateRow) -> Result<Self, Self::Error> {
        // Parse the metadata field from the row, which contains all the data we need to build the
        // replication phase
        let Some(table_replication_state) = value.deserialize_metadata().map_err(|err| {
            etl_error!(
                ErrorKind::DeserializationError,
                "Table replication state deserialization failed",
                format!(
                    "Failed to deserialize table replication state from metadata column in PostgreSQL: {}", err
                )
            )
        })?
        else {
            bail!(
                ErrorKind::InvalidState,
                "Table replication state not found",
                "Table replication state does not exist in metadata column in PostgreSQL"
            );
        };

        // Convert postgres state to phase (they are the same structs but one is meant to represent
        // only the state which can be saved in the db).
        match table_replication_state {
            state::TableReplicationState::Init => Ok(TableReplicationPhase::Init),
            state::TableReplicationState::DataSync => Ok(TableReplicationPhase::DataSync),
            state::TableReplicationState::FinishedCopy => Ok(TableReplicationPhase::FinishedCopy),
            state::TableReplicationState::SyncDone { lsn } => {
                Ok(TableReplicationPhase::SyncDone { lsn })
            }
            state::TableReplicationState::Ready => Ok(TableReplicationPhase::Ready),
            state::TableReplicationState::Errored {
                reason,
                solution,
                retry_policy,
            } => {
                let etl_retry_policy = match retry_policy {
                    state::RetryPolicy::NoRetry => RetryPolicy::NoRetry,
                    state::RetryPolicy::ManualRetry => RetryPolicy::ManualRetry,
                    state::RetryPolicy::TimedRetry { next_retry } => {
                        RetryPolicy::TimedRetry { next_retry }
                    }
                };

                Ok(TableReplicationPhase::Errored {
                    reason,
                    solution,
                    retry_policy: etl_retry_policy,
                })
            }
        }
    }
}

/// A variant of [`TableReplicationPhase`] that can be used to determine the current phase of a table
/// without having to pattern match on the data fields.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TableReplicationPhaseType {
    Init,
    DataSync,
    FinishedCopy,
    SyncWait,
    Catchup,
    SyncDone,
    Ready,
    Errored,
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
            Self::Errored => true,
        }
    }

    /// Returns `true` if a table with this phase is done processing, `false` otherwise.
    ///
    /// A table is done processing, when its events are being processed by the apply worker instead
    /// of a table sync worker or when it has errored.
    pub fn is_done(&self) -> bool {
        match self {
            Self::Init => false,
            Self::DataSync => false,
            Self::FinishedCopy => false,
            Self::SyncWait => false,
            Self::Catchup => false,
            Self::SyncDone => false,
            Self::Ready => true,
            Self::Errored => true,
        }
    }

    /// Return `true` if a table with this phase is in error, `false` otherwise.
    pub fn is_errored(&self) -> bool {
        matches!(self, Self::Errored)
    }

    pub fn as_static_str(&self) -> &'static str {
        match self {
            TableReplicationPhaseType::Init => "init",
            TableReplicationPhaseType::DataSync => "data_sync",
            TableReplicationPhaseType::FinishedCopy => "finished_copy",
            TableReplicationPhaseType::SyncWait => "sync_wait",
            TableReplicationPhaseType::Catchup => "catchup",
            TableReplicationPhaseType::SyncDone => "sync_done",
            TableReplicationPhaseType::Ready => "ready",
            TableReplicationPhaseType::Errored => "errored",
        }
    }
}

impl<'a> From<&'a TableReplicationPhase> for TableReplicationPhaseType {
    fn from(phase: &'a TableReplicationPhase) -> Self {
        match phase {
            TableReplicationPhase::Init => Self::Init,
            TableReplicationPhase::DataSync => Self::DataSync,
            TableReplicationPhase::FinishedCopy => Self::FinishedCopy,
            TableReplicationPhase::SyncWait { .. } => Self::SyncWait,
            TableReplicationPhase::Catchup { .. } => Self::Catchup,
            TableReplicationPhase::SyncDone { .. } => Self::SyncDone,
            TableReplicationPhase::Ready => Self::Ready,
            TableReplicationPhase::Errored { .. } => Self::Errored,
        }
    }
}

impl fmt::Display for TableReplicationPhaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_static_str())
    }
}
