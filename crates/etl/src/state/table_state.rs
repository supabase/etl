use std::fmt;

use etl_postgres::replication::table_state::{StoredTableStateType, TableStateRow};
use serde::{Deserialize, Serialize};

use crate::{
    bail,
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    state::{TableError, TableRetryPolicy},
    types::PgLsn,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TableState {
    /// Set by the pipeline when it first starts and encounters a table for the
    /// first time.
    Init,
    /// Set by table-sync worker just before starting initial table copy.
    DataSync,
    /// Set by table-sync worker when initial table copy is done.
    FinishedCopy,
    /// Set by table-sync worker when waiting for the apply worker to pause.
    ///
    /// The apply worker checks for `SyncWait` at transaction boundaries and
    /// while idle before reading more WAL. When found, it moves the worker to
    /// `Catchup` and waits for the table-sync worker to reach `SyncDone` or
    /// `Errored`.
    ///
    /// This state is stored in memory only and not persisted to the state
    /// store.
    SyncWait {
        /// The LSN of the snapshot used for the initial table copy.
        ///
        /// This LSN represents the consistent point from which the table sync
        /// worker will start streaming changes. The apply worker will use
        /// `max(this LSN, current_lsn)` when setting the Catchup LSN to ensure
        /// no data loss, following PostgreSQL's pattern.
        #[serde(with = "lsn_serde")]
        lsn: PgLsn,
    },
    /// Set by the apply worker when it is paused. The table-sync worker waits
    /// for the apply worker to set this state after setting the state to
    /// `SyncWait`. A restarted apply loop that finds an active worker already
    /// in `Catchup` must wait for that worker before reading more WAL.
    ///
    /// This state is stored in memory only and not persisted to the state
    /// store.
    Catchup {
        /// The LSN to catch up before shutting down the table sync worker and
        /// handing over streaming to the apply worker.
        #[serde(with = "lsn_serde")]
        lsn: PgLsn,
    },

    /// Set by the table-sync worker when catch-up work is completed and the
    /// table-sync worker has caught up with the apply worker's LSN position.
    ///
    /// The apply worker waits for this state before continuing to process
    /// events for the table.
    SyncDone {
        /// The LSN up to which the table-sync worker has caught up.
        ///
        /// This LSN is guaranteed to be >= `Catchup.lsn`.
        #[serde(with = "lsn_serde")]
        lsn: PgLsn,
    },
    /// Set by apply worker when it has caught up with the table-sync worker's
    /// catch-up LSN position. Tables with this state have successfully run
    /// their initial table copy and catch-up work and any changes to them
    /// will now be applied by the apply worker only.
    Ready,
    /// Set by either the table-sync worker or the apply worker when a table
    /// encounters an error during replication. Contains diagnostic information
    /// and retry policy.
    Errored {
        /// Human-readable description of what went wrong.
        reason: String,
        /// Optional suggestion for how to fix the issue.
        solution: Option<String>,
        /// Retry policy specifying how/when to retry.
        retry_policy: TableRetryPolicy,
        /// Original error that triggered the table error state.
        ///
        /// This field is **not persisted** — it is skipped during
        /// serialization and replaced with a generic placeholder on
        /// deserialization. Code that reads states from the state store should
        /// not rely on `source_err` containing the original error; it is only
        /// meaningful for the in-memory lifetime of the state that produced it.
        #[serde(skip, default = "default_source_err")]
        source_err: EtlError,
    },
}

/// Builds a fallback source error when restoring state from storage.
fn default_source_err() -> EtlError {
    etl_error!(ErrorKind::Unknown, "Table replication error restored from state store")
}

impl TableState {
    /// Returns this state's type without associated data.
    pub fn as_type(&self) -> TableStateType {
        self.into()
    }

    /// Returns whether this state represents an errored state.
    pub fn is_errored(&self) -> bool {
        matches!(self, Self::Errored { .. })
    }

    /// Converts this state to the database storage format.
    ///
    /// Returns the state type enum and serialized JSON metadata for persisting
    /// to the state store. Returns an error for in-memory-only states that
    /// cannot be persisted.
    pub fn to_storage_format(&self) -> EtlResult<(StoredTableStateType, serde_json::Value)> {
        let state_type = self.as_type();
        if !state_type.should_store() {
            bail!(
                ErrorKind::InvalidState,
                "In-memory table state cannot be persisted",
                "In-memory table states (SyncWait, Catchup) cannot be saved to state store"
            );
        }

        let state_type: StoredTableStateType = state_type.try_into()?;
        let metadata = serde_json::to_value(self).map_err(|err| {
            etl_error!(
                ErrorKind::SerializationError,
                "Table state serialization failed",
                source: err
            )
        })?;

        Ok((state_type, metadata))
    }

    /// Deserializes a [`TableState`] from a state store row's
    /// metadata.
    pub fn from_state_row(row: TableStateRow) -> EtlResult<Self> {
        let Some(metadata) = row.metadata else {
            bail!(
                ErrorKind::InvalidState,
                "Table state not found",
                "Table state does not exist in metadata column in PostgreSQL"
            );
        };

        serde_json::from_value(metadata).map_err(|err| {
            etl_error!(
                ErrorKind::DeserializationError,
                "Table state deserialization failed",
                format!(
                    "Failed to deserialize table state from metadata column in PostgreSQL: {err}"
                )
            )
        })
    }
}

impl fmt::Display for TableState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Init => write!(f, "init"),
            Self::DataSync => write!(f, "data_sync"),
            Self::FinishedCopy => write!(f, "finished_copy"),
            Self::SyncWait { lsn } => write!(f, "sync_wait({lsn})"),
            Self::Catchup { lsn } => write!(f, "catchup({lsn})"),
            Self::SyncDone { lsn } => write!(f, "sync_done({lsn})"),
            Self::Ready => write!(f, "ready"),
            Self::Errored { .. } => write!(f, "errored"),
        }
    }
}

impl From<TableError> for TableState {
    fn from(value: TableError) -> Self {
        Self::Errored {
            reason: value.reason,
            solution: value.solution,
            retry_policy: value.retry_policy,
            source_err: value.source_err,
        }
    }
}

/// A variant of [`TableState`] that can be used to determine the
/// current state of a table without having to pattern match on the data fields.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum TableStateType {
    Init,
    DataSync,
    FinishedCopy,
    SyncWait,
    Catchup,
    SyncDone,
    Ready,
    Errored,
}

impl TableStateType {
    /// Returns `true` if the state should be saved into the state store,
    /// `false` otherwise.
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

    /// Returns `true` if a table with this state is done processing, `false`
    /// otherwise.
    ///
    /// A table is done processing, when its events are being processed by the
    /// apply worker instead of a table sync worker or when it has errored.
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

    /// Returns `true` if a table with this state is in error, `false`
    /// otherwise.
    pub fn is_errored(&self) -> bool {
        matches!(self, Self::Errored)
    }
}

impl From<TableStateType> for &'static str {
    fn from(value: TableStateType) -> Self {
        match value {
            TableStateType::Init => "init",
            TableStateType::DataSync => "data_sync",
            TableStateType::FinishedCopy => "finished_copy",
            TableStateType::SyncWait => "sync_wait",
            TableStateType::Catchup => "catchup",
            TableStateType::SyncDone => "sync_done",
            TableStateType::Ready => "ready",
            TableStateType::Errored => "errored",
        }
    }
}

impl TryFrom<TableStateType> for StoredTableStateType {
    type Error = EtlError;

    fn try_from(value: TableStateType) -> Result<Self, Self::Error> {
        match value {
            TableStateType::Init => Ok(Self::Init),
            TableStateType::DataSync => Ok(Self::DataSync),
            TableStateType::FinishedCopy => Ok(Self::FinishedCopy),
            TableStateType::SyncDone => Ok(Self::SyncDone),
            TableStateType::Ready => Ok(Self::Ready),
            TableStateType::Errored => Ok(Self::Errored),
            TableStateType::SyncWait | TableStateType::Catchup => Err(etl_error!(
                ErrorKind::InvalidState,
                "In-memory table state cannot be converted to storage state",
                "In-memory table states (SyncWait, Catchup) cannot be saved to state store"
            )),
        }
    }
}

impl From<&TableState> for TableStateType {
    fn from(state: &TableState) -> Self {
        match state {
            TableState::Init => Self::Init,
            TableState::DataSync => Self::DataSync,
            TableState::FinishedCopy => Self::FinishedCopy,
            TableState::SyncWait { .. } => Self::SyncWait,
            TableState::Catchup { .. } => Self::Catchup,
            TableState::SyncDone { .. } => Self::SyncDone,
            TableState::Ready => Self::Ready,
            TableState::Errored { .. } => Self::Errored,
        }
    }
}

impl fmt::Display for TableStateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value: &'static str = (*self).into();
        f.write_str(value)
    }
}

/// Serde serialization helpers for Postgres LSN values.
mod lsn_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use tokio_postgres::types::PgLsn;

    pub(super) fn serialize<S>(lsn: &PgLsn, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        lsn.to_string().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<PgLsn, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(|e| serde::de::Error::custom(format!("{e:?}")))
    }
}

#[cfg(test)]
mod tests {
    use etl_postgres::replication::table_state;
    use tokio_postgres::types::PgLsn;

    use crate::{
        error::ErrorKind,
        etl_error,
        state::{TableRetryPolicy, TableState, TableStateType},
    };

    #[test]
    fn table_state_serialization() {
        let init = TableState::Init;
        let json = serde_json::to_value(&init).unwrap();
        assert_eq!(json, serde_json::json!({"type": "init"}));
        let deserialized: TableState = serde_json::from_value(json).unwrap();
        assert!(matches!(deserialized, TableState::Init));

        let lsn = "0/1000000".parse::<PgLsn>().unwrap();
        let sync_done = TableState::SyncDone { lsn };
        let json = serde_json::to_value(&sync_done).unwrap();
        assert_eq!(json, serde_json::json!({"type": "sync_done", "lsn": "0/1000000"}));
        let deserialized: TableState = serde_json::from_value(json).unwrap();
        if let TableState::SyncDone { lsn: got } = deserialized {
            assert_eq!(got, lsn);
        } else {
            panic!("Expected SyncDone variant");
        }

        let errored = TableState::Errored {
            reason: "Test error".to_owned(),
            solution: Some("Test solution".to_owned()),
            retry_policy: TableRetryPolicy::NoRetry,
            source_err: etl_error!(ErrorKind::Unknown, "Test"),
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
        assert!(!json.to_string().contains("source_err"));

        let deserialized: TableState = serde_json::from_value(json).unwrap();
        if let TableState::Errored { reason, solution, retry_policy, .. } = deserialized {
            assert_eq!(reason, "Test error");
            assert_eq!(solution, Some("Test solution".to_owned()));
            assert!(matches!(retry_policy, TableRetryPolicy::NoRetry));
        } else {
            panic!("Expected Errored variant");
        }
    }

    #[test]
    fn to_storage_format_rejects_memory_only_states() {
        let sync_wait = TableState::SyncWait { lsn: "0/1000".parse::<PgLsn>().unwrap() };
        assert!(sync_wait.to_storage_format().is_err());

        let catchup = TableState::Catchup { lsn: "0/2000".parse::<PgLsn>().unwrap() };
        assert!(catchup.to_storage_format().is_err());
    }

    #[test]
    fn state_type_converts_to_static_label() {
        let label: &'static str = TableStateType::Ready.into();

        assert_eq!(label, "ready");
    }

    #[test]
    fn state_type_converts_to_postgres_state_type() {
        let state_type: table_state::StoredTableStateType =
            TableStateType::Ready.try_into().unwrap();

        assert_eq!(state_type, table_state::StoredTableStateType::Ready);
    }

    #[test]
    fn from_state_row_fails_on_missing_metadata() {
        let row = table_state::TableStateRow {
            id: 1,
            pipeline_id: 1,
            table_id: sqlx::postgres::types::Oid(42),
            state: table_state::StoredTableStateType::Init,
            metadata: None,
            prev: None,
            is_current: true,
        };
        assert!(TableState::from_state_row(row).is_err());
    }

    #[test]
    fn from_state_row_fails_on_invalid_json() {
        let row = table_state::TableStateRow {
            id: 1,
            pipeline_id: 1,
            table_id: sqlx::postgres::types::Oid(42),
            state: table_state::StoredTableStateType::Init,
            metadata: Some(serde_json::json!({"type": "nonexistent_variant"})),
            prev: None,
            is_current: true,
        };
        assert!(TableState::from_state_row(row).is_err());
    }

    #[test]
    fn from_state_row_round_trip() {
        let states = vec![
            TableState::Init,
            TableState::DataSync,
            TableState::FinishedCopy,
            TableState::SyncDone { lsn: "0/1000000".parse::<PgLsn>().unwrap() },
            TableState::Ready,
            TableState::Errored {
                reason: "broken".to_owned(),
                solution: Some("fix it".to_owned()),
                retry_policy: TableRetryPolicy::ManualRetry,
                source_err: etl_error!(ErrorKind::Unknown, "Test"),
            },
        ];

        for state in states {
            let (state_type, metadata) = state.to_storage_format().unwrap();
            let row = table_state::TableStateRow {
                id: 1,
                pipeline_id: 1,
                table_id: sqlx::postgres::types::Oid(42),
                state: state_type,
                metadata: Some(metadata),
                prev: None,
                is_current: true,
            };
            let restored = TableState::from_state_row(row).unwrap();

            assert_eq!(state.as_type(), restored.as_type());
            match (&state, &restored) {
                (TableState::SyncDone { lsn: a }, TableState::SyncDone { lsn: b }) => {
                    assert_eq!(a, b);
                }
                (
                    TableState::Errored { reason: r1, solution: s1, retry_policy: rp1, .. },
                    TableState::Errored { reason: r2, solution: s2, retry_policy: rp2, .. },
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
