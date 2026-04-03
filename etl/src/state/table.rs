use chrono::{DateTime, Duration, Utc};
use etl_postgres::replication::state;
use etl_postgres::types::TableId;
use serde::{Deserialize, Serialize};
use std::fmt;
use tokio_postgres::types::PgLsn;

use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::workers::policy::ErrorHandlingPolicy;
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
    source_err: EtlError,
}

impl TableReplicationError {
    /// Creates a new [`TableReplicationError`] with a suggested solution.
    pub fn with_solution(
        table_id: TableId,
        reason: String,
        solution: String,
        retry_policy: RetryPolicy,
        source_err: EtlError,
    ) -> Self {
        Self {
            table_id,
            reason,
            solution: Some(solution),
            retry_policy,
            source_err,
        }
    }

    /// Creates a new [`TableReplicationError`] without a suggested solution.
    pub fn without_solution(
        table_id: TableId,
        reason: String,
        retry_policy: RetryPolicy,
        source_err: EtlError,
    ) -> Self {
        Self {
            table_id,
            reason,
            solution: None,
            retry_policy,
            source_err,
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

    /// Builds a [`TableReplicationError`] from a shared handling policy and worker retry policy.
    pub fn from_error_policy(
        table_id: TableId,
        err: EtlError,
        policy: &ErrorHandlingPolicy,
        retry_policy: RetryPolicy,
    ) -> Self {
        match policy.solution() {
            Some(solution) => Self::with_solution(
                table_id,
                err.to_string(),
                solution.to_string(),
                retry_policy,
                err,
            ),
            None => Self::without_solution(table_id, err.to_string(), retry_policy, err),
        }
    }
}

/// Defines the retry strategy for a failed table replication.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
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
        /// when setting the Catchup LSN to ensure no data loss, following PostgreSQL's pattern.
        #[serde(with = "lsn_serde")]
        lsn: PgLsn,
    },
    /// Set by the apply worker when it is paused. The table-sync worker waits
    /// for the apply worker to set this state after setting the state to `SyncWait`.
    ///
    /// This phase is stored in memory only and not persisted to the state store
    Catchup {
        /// The lsn to catch up before shutting down the table sync worker and handing over streaming
        /// to the apply worker.
        #[serde(with = "lsn_serde")]
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
        #[serde(with = "lsn_serde")]
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
        /// Original error that triggered the table error state.
        ///
        /// This field is **not persisted** — it is skipped during serialization and
        /// replaced with a generic placeholder on deserialization. Code that reads
        /// phases from the state store should not rely on `source_err` containing
        /// the original error; it is only meaningful for the in-memory lifetime of
        /// the phase that produced it.
        #[serde(skip, default = "default_source_err")]
        source_err: EtlError,
    },
}

fn default_source_err() -> EtlError {
    etl_error!(
        ErrorKind::Unknown,
        "table replication error restored from state store"
    )
}

impl TableReplicationPhase {
    pub fn as_type(&self) -> TableReplicationPhaseType {
        self.into()
    }

    /// Returns whether this phase represents an errored state.
    pub fn is_errored(&self) -> bool {
        matches!(self, Self::Errored { .. })
    }

    /// Converts this phase to the database storage format.
    ///
    /// Returns the state type enum and serialized JSON metadata for persisting
    /// to the state store. Returns an error for in-memory-only phases that
    /// cannot be persisted.
    pub fn to_storage_format(
        &self,
    ) -> EtlResult<(state::TableReplicationStateType, serde_json::Value)> {
        if !self.as_type().should_store() {
            bail!(
                ErrorKind::InvalidState,
                "In-memory replication phase cannot be persisted",
                "In-memory table replication phases (SyncWait, Catchup) cannot be saved to state store"
            );
        }

        let state_type = match self.as_type() {
            TableReplicationPhaseType::Init => state::TableReplicationStateType::Init,
            TableReplicationPhaseType::DataSync => state::TableReplicationStateType::DataSync,
            TableReplicationPhaseType::FinishedCopy => {
                state::TableReplicationStateType::FinishedCopy
            }
            TableReplicationPhaseType::SyncDone => state::TableReplicationStateType::SyncDone,
            TableReplicationPhaseType::Ready => state::TableReplicationStateType::Ready,
            TableReplicationPhaseType::Errored => state::TableReplicationStateType::Errored,
            // Covered by should_store() check above.
            TableReplicationPhaseType::SyncWait | TableReplicationPhaseType::Catchup => {
                unreachable!()
            }
        };

        let metadata = serde_json::to_value(self).map_err(|err| {
            etl_error!(
                ErrorKind::SerializationError,
                "Table replication phase serialization failed",
                format!("Failed to serialize table replication phase to JSON: {err}")
            )
        })?;

        Ok((state_type, metadata))
    }

    /// Deserializes a [`TableReplicationPhase`] from a state store row's metadata.
    pub fn from_state_row(row: &state::TableReplicationStateRow) -> EtlResult<Self> {
        let Some(metadata) = &row.metadata else {
            bail!(
                ErrorKind::InvalidState,
                "Table replication state not found",
                "Table replication state does not exist in metadata column in PostgreSQL"
            );
        };

        serde_json::from_value(metadata.clone()).map_err(|err| {
            etl_error!(
                ErrorKind::DeserializationError,
                "Table replication state deserialization failed",
                format!(
                    "Failed to deserialize table replication state from metadata column in PostgreSQL: {err}"
                )
            )
        })
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
            source_err: value.source_err,
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

/// Serde serialization helpers for Postgres LSN values.
mod lsn_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use tokio_postgres::types::PgLsn;

    pub fn serialize<S>(lsn: &PgLsn, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        lsn.to_string().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PgLsn, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse()
            .map_err(|e| serde::de::Error::custom(format!("{e:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tokio_postgres::types::PgLsn;

    #[test]
    fn retry_policy_serialization() {
        let no_retry = RetryPolicy::NoRetry;
        let json = serde_json::to_value(&no_retry).unwrap();
        assert_eq!(json, serde_json::json!({"type": "no_retry"}));
        let deserialized: RetryPolicy = serde_json::from_value(json).unwrap();
        assert!(matches!(deserialized, RetryPolicy::NoRetry));

        let manual_retry = RetryPolicy::ManualRetry;
        let json = serde_json::to_value(&manual_retry).unwrap();
        assert_eq!(json, serde_json::json!({"type": "manual_retry"}));
        let deserialized: RetryPolicy = serde_json::from_value(json).unwrap();
        assert!(matches!(deserialized, RetryPolicy::ManualRetry));

        let timestamp = Utc::now();
        let timed_retry = RetryPolicy::TimedRetry {
            next_retry: timestamp,
        };
        let json = serde_json::to_value(&timed_retry).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"type": "timed_retry", "next_retry": timestamp})
        );
        let deserialized: RetryPolicy = serde_json::from_value(json).unwrap();
        if let RetryPolicy::TimedRetry { next_retry } = deserialized {
            assert_eq!(next_retry, timestamp);
        } else {
            panic!("Expected TimedRetry variant");
        }
    }

    #[test]
    fn table_replication_phase_serialization() {
        // Init round-trip
        let init = TableReplicationPhase::Init;
        let json = serde_json::to_value(&init).unwrap();
        assert_eq!(json, serde_json::json!({"type": "init"}));
        let deserialized: TableReplicationPhase = serde_json::from_value(json).unwrap();
        assert!(matches!(deserialized, TableReplicationPhase::Init));

        // SyncDone round-trip (exercises lsn_serde)
        let lsn = "0/1000000".parse::<PgLsn>().unwrap();
        let sync_done = TableReplicationPhase::SyncDone { lsn };
        let json = serde_json::to_value(&sync_done).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"type": "sync_done", "lsn": "0/1000000"})
        );
        let deserialized: TableReplicationPhase = serde_json::from_value(json).unwrap();
        if let TableReplicationPhase::SyncDone { lsn: got } = deserialized {
            assert_eq!(got, lsn);
        } else {
            panic!("Expected SyncDone variant");
        }

        // Errored round-trip (source_err is skipped)
        let errored = TableReplicationPhase::Errored {
            reason: "Test error".to_string(),
            solution: Some("Test solution".to_string()),
            retry_policy: RetryPolicy::NoRetry,
            source_err: etl_error!(ErrorKind::Unknown, "test"),
        };
        let json = serde_json::to_value(&errored).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "type": "errored",
                "reason": "Test error",
                "solution": "Test solution",
                "retry_policy": {"type": "no_retry"}
            })
        );
        // source_err should NOT appear in the JSON
        assert!(!json.to_string().contains("source_err"));

        let deserialized: TableReplicationPhase = serde_json::from_value(json).unwrap();
        if let TableReplicationPhase::Errored {
            reason,
            solution,
            retry_policy,
            ..
        } = deserialized
        {
            assert_eq!(reason, "Test error");
            assert_eq!(solution, Some("Test solution".to_string()));
            assert!(matches!(retry_policy, RetryPolicy::NoRetry));
        } else {
            panic!("Expected Errored variant");
        }
    }

    #[test]
    fn to_storage_format_rejects_memory_only_phases() {
        let sync_wait = TableReplicationPhase::SyncWait {
            lsn: "0/1000".parse::<PgLsn>().unwrap(),
        };
        assert!(sync_wait.to_storage_format().is_err());

        let catchup = TableReplicationPhase::Catchup {
            lsn: "0/2000".parse::<PgLsn>().unwrap(),
        };
        assert!(catchup.to_storage_format().is_err());
    }

    #[test]
    fn from_state_row_fails_on_missing_metadata() {
        let row = state::TableReplicationStateRow {
            id: 1,
            pipeline_id: 1,
            table_id: sqlx::postgres::types::Oid(42),
            state: state::TableReplicationStateType::Init,
            metadata: None,
            prev: None,
            is_current: true,
        };
        assert!(TableReplicationPhase::from_state_row(&row).is_err());
    }

    #[test]
    fn from_state_row_fails_on_invalid_json() {
        let row = state::TableReplicationStateRow {
            id: 1,
            pipeline_id: 1,
            table_id: sqlx::postgres::types::Oid(42),
            state: state::TableReplicationStateType::Init,
            metadata: Some(serde_json::json!({"type": "nonexistent_variant"})),
            prev: None,
            is_current: true,
        };
        assert!(TableReplicationPhase::from_state_row(&row).is_err());
    }

    #[test]
    fn from_state_row_round_trip() {
        let phases = vec![
            TableReplicationPhase::Init,
            TableReplicationPhase::DataSync,
            TableReplicationPhase::FinishedCopy,
            TableReplicationPhase::SyncDone {
                lsn: "0/1000000".parse::<PgLsn>().unwrap(),
            },
            TableReplicationPhase::Ready,
            TableReplicationPhase::Errored {
                reason: "broken".to_string(),
                solution: Some("fix it".to_string()),
                retry_policy: RetryPolicy::ManualRetry,
                source_err: etl_error!(ErrorKind::Unknown, "test"),
            },
        ];

        for phase in phases {
            let (state_type, metadata) = phase.to_storage_format().unwrap();
            let row = state::TableReplicationStateRow {
                id: 1,
                pipeline_id: 1,
                table_id: sqlx::postgres::types::Oid(42),
                state: state_type,
                metadata: Some(metadata),
                prev: None,
                is_current: true,
            };
            let restored = TableReplicationPhase::from_state_row(&row).unwrap();

            // Compare everything except source_err (which is lossy by design)
            assert_eq!(phase.as_type(), restored.as_type());
            match (&phase, &restored) {
                (
                    TableReplicationPhase::SyncDone { lsn: a },
                    TableReplicationPhase::SyncDone { lsn: b },
                ) => assert_eq!(a, b),
                (
                    TableReplicationPhase::Errored {
                        reason: r1,
                        solution: s1,
                        retry_policy: rp1,
                        ..
                    },
                    TableReplicationPhase::Errored {
                        reason: r2,
                        solution: s2,
                        retry_policy: rp2,
                        ..
                    },
                ) => {
                    assert_eq!(r1, r2);
                    assert_eq!(s1, s2);
                    assert_eq!(rp1, rp2);
                }
                _ => {}
            }
        }
    }
}
