use std::{fmt, sync::Arc};

use etl_postgres::store::table_state::{StoredTableStateRow, StoredTableStateType};
use serde::{Deserialize, Serialize};
use tokio_postgres::types::PgLsn;

use crate::{
    bail,
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    replication::state::{TableError, TableRetryPolicy},
    schema::{IdentityMask, ReplicatedTableSchema, ReplicationMask, SnapshotId, TableSchema},
};

/// Compact durable form of `crate::replication::TableDecodingState` stored at
/// `SyncDone`.
///
/// The three fields are serialized together inside
/// `SyncDone.table_decoding_state`. This stores the exact schema snapshot and
/// masks needed to reconstruct `TableDecodingState::WithSchema`, but does not
/// duplicate the full [`TableSchema`], which remains in the schema store. The
/// masks use the same ordered raw bytes stored in destination table metadata;
/// JSON represents those bytes as arrays of `0` and `1` values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredTableDecodingState {
    /// Exact schema snapshot used by the table-sync decoder.
    #[serde(with = "snapshot_id_serde")]
    snapshot_id: SnapshotId,
    /// Publication-column membership in table-schema order.
    replication_mask: Vec<u8>,
    /// Replica-identity membership in table-schema order.
    identity_mask: Vec<u8>,
}

impl StoredTableDecodingState {
    /// Captures decoding state from a materialized replicated schema.
    fn from_replicated_table_schema(schema: &ReplicatedTableSchema) -> Self {
        Self {
            snapshot_id: schema.inner().snapshot_id,
            replication_mask: schema.replication_mask().to_bytes(),
            identity_mask: schema.identity_mask().to_bytes(),
        }
    }

    /// Returns the exact schema snapshot used by this decoding state.
    pub(crate) fn snapshot_id(&self) -> SnapshotId {
        self.snapshot_id
    }

    /// Materializes and validates this decoding state against its stored
    /// schema.
    pub(crate) fn materialize(
        &self,
        table_schema: Arc<TableSchema>,
        sync_done_lsn: PgLsn,
    ) -> EtlResult<ReplicatedTableSchema> {
        // Exact lookup proves that the snapshot exists, but not that it was
        // reachable by this handover. Durable JSON can also outlive the writer
        // version or be malformed independently, so validate the complete
        // stored representation before rebuilding unchecked mask types.
        let sync_done_snapshot_frontier = SnapshotId::before_lsn(sync_done_lsn);
        if self.snapshot_id > sync_done_snapshot_frontier {
            bail!(
                ErrorKind::InvalidState,
                "Table-schema decoding snapshot is ahead of SyncDone",
                format!(
                    "Table {} decoding snapshot {} exceeds SyncDone LSN {}",
                    table_schema.id, self.snapshot_id, sync_done_lsn
                )
            );
        }

        let column_count = table_schema.column_schemas.len();
        if self.replication_mask.len() != column_count || self.identity_mask.len() != column_count {
            bail!(
                ErrorKind::InvalidState,
                "Table-schema decoding mask width does not match its schema",
                format!(
                    "Table {} snapshot {} has {} columns, replication mask width {}, and identity \
                     mask width {}",
                    table_schema.id,
                    self.snapshot_id,
                    column_count,
                    self.replication_mask.len(),
                    self.identity_mask.len()
                )
            );
        }
        if self.replication_mask.iter().chain(&self.identity_mask).any(|value| *value > 1) {
            bail!(
                ErrorKind::InvalidState,
                "Table-schema decoding state contains a non-binary mask"
            );
        }
        if self
            .replication_mask
            .iter()
            .zip(&self.identity_mask)
            .any(|(replicated, identity)| *identity == 1 && *replicated == 0)
        {
            bail!(
                ErrorKind::InvalidState,
                "Table-schema decoding identity mask is not a subset of its replication mask"
            );
        }

        Ok(ReplicatedTableSchema::from_masks(
            table_schema,
            ReplicationMask::from_bytes(self.replication_mask.clone()),
            IdentityMask::from_bytes(self.identity_mask.clone()),
        ))
    }
}

/// Replication lifecycle state for a source table.
///
/// Table states coordinate initial table copy, catch-up, steady-state
/// streaming, and per-table errors. Some states are durable and stored in the
/// configured [`crate::store::StateStore`], while transitional coordination
/// states are memory-only.
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
        /// The exclusive LSN up to which the table-sync worker has caught up.
        ///
        /// This LSN is guaranteed to be >= `Catchup.lsn`. A transaction whose
        /// commit record starts here belongs to the apply worker; its schema
        /// changes are not part of the stored handoff decoder.
        #[serde(with = "lsn_serde")]
        lsn: PgLsn,
        /// Compact durable decoding state captured at `SyncDone`.
        ///
        /// `None` is accepted only for compatibility with `SyncDone` rows
        /// written before decoding state was persisted. New rows always store
        /// `Some`, whose type guarantees that the snapshot ID and both masks
        /// are present together. An eventless table may remain in this state
        /// indefinitely so the first future row can restore this exact decoder
        /// without adding another retained-decoder lifecycle state.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        table_decoding_state: Option<StoredTableDecodingState>,
    },
    /// Set by apply worker when it has caught up with the table-sync worker's
    /// catch-up LSN position. Tables with this state have successfully run
    /// their initial table copy and catch-up work and any changes to them will
    /// now be applied by the apply worker only.
    ///
    /// `Ready` no longer retains the [`StoredTableDecodingState`] from
    /// `SyncDone`. Before persisting `Ready`, the apply worker therefore
    /// requires the current connection to have crossed `SyncDone.lsn`, a
    /// durable apply checkpoint at or beyond that boundary, and decoding state
    /// materialized as `WithSchema`. That local state proves this connection
    /// either received a relation or restored the complete `SyncDone` decoder
    /// while handling relation-less DML.
    ///
    /// These conditions protect different attempts. The local decoder protects
    /// only the current connection, which may not receive another relation
    /// message after handover. Restart safety comes from the durable
    /// checkpoint: the next apply worker starts at the later of its
    /// replication-slot position and that checkpoint, so its bootstrap is at or
    /// beyond `SyncDone.lsn`. A fresh pgoutput connection emits relation
    /// metadata before its first row change, allowing it to resolve the newest
    /// stored schema committed strictly before the restart position.
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
        /// This field is **not persisted** — it is skipped during serialization
        /// and replaced with a generic placeholder on deserialization. Code
        /// that reads states from the state store should not rely on
        /// `source_err` containing the original error; it is only meaningful
        /// for the in-memory lifetime of the state that produced it.
        #[serde(skip, default = "default_source_err")]
        source_err: EtlError,
    },
}

/// Builds a fallback source error when restoring state from storage.
fn default_source_err() -> EtlError {
    etl_error!(ErrorKind::Unknown, "Table replication error restored from state store")
}

impl TableState {
    /// Creates a durable `SyncDone` state from the table-sync decoder.
    ///
    /// The snapshot and both masks form one logical decoding state and are
    /// always stored together for newly written rows.
    pub(crate) fn sync_done(lsn: PgLsn, schema: &ReplicatedTableSchema) -> Self {
        Self::SyncDone {
            lsn,
            table_decoding_state: Some(StoredTableDecodingState::from_replicated_table_schema(
                schema,
            )),
        }
    }

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

        let state_type = state_type.to_storage_type()?;
        let metadata = serde_json::to_value(self).map_err(|err| {
            etl_error!(
                ErrorKind::SerializationError,
                "Table state serialization failed",
                source: err
            )
        })?;

        Ok((state_type, metadata))
    }

    /// Deserializes a [`TableState`] from a state store row's metadata.
    pub(crate) fn from_state_row(row: StoredTableStateRow) -> EtlResult<Self> {
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
            Self::SyncDone { lsn, .. } => write!(f, "sync_done({lsn})"),
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

/// A variant of [`TableState`] that can be used to determine the current state
/// of a table without having to pattern match on the data fields.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum TableStateType {
    /// Table has been discovered but no copy work has started.
    Init,
    /// Table is currently being copied by a table sync worker.
    DataSync,
    /// Initial table copy completed.
    FinishedCopy,
    /// Table sync is waiting for the apply worker to pause.
    SyncWait,
    /// Apply worker is paused while the table sync worker catches up.
    Catchup,
    /// Table sync worker caught up to the handoff LSN.
    SyncDone,
    /// Table is ready for steady-state apply-worker replication.
    Ready,
    /// Table replication is stopped on an error.
    Errored,
}

impl TableStateType {
    /// Returns `true` if the state should be saved into the state store,
    /// `false` otherwise.
    pub fn should_store(&self) -> bool {
        !matches!(self, Self::SyncWait | Self::Catchup)
    }

    /// Returns whether a table with this state is still synchronizing.
    ///
    /// Synchronization includes the handoff to the apply worker, so
    /// [`TableStateType::SyncDone`] remains a syncing state until it
    /// transitions to [`TableStateType::Ready`]. Errored tables are not
    /// actively syncing.
    pub fn is_syncing(&self) -> bool {
        !matches!(self, Self::Ready | Self::Errored)
    }

    /// Returns whether a table has completed its initial table sync.
    ///
    /// Tables in these states keep their existing destination data when the
    /// pipeline restarts. Earlier non-error syncing states restart the table
    /// copy from a fresh snapshot. Errored tables are neither completed nor
    /// automatically recopied.
    ///
    /// [`TableStateType::FinishedCopy`] is deliberately excluded because the
    /// bulk copy completed without a durable catchup handoff, so restart
    /// repeats the copy. [`TableStateType::SyncDone`] is included because
    /// catchup completed durably even though the final transition to
    /// [`TableStateType::Ready`] remains.
    pub fn has_completed_table_sync(&self) -> bool {
        matches!(self, Self::SyncDone | Self::Ready)
    }

    /// Returns `true` if a table with this state would perform the initial
    /// table sync, `false` otherwise.
    pub fn would_perform_table_sync(&self) -> bool {
        !matches!(self, Self::SyncDone | Self::Ready | Self::Errored)
    }

    /// Returns `true` if a table with this state is in error, `false`
    /// otherwise.
    pub fn is_errored(&self) -> bool {
        matches!(self, Self::Errored)
    }

    /// Converts this public state type to its persistent storage enum.
    pub(crate) fn to_storage_type(self) -> EtlResult<StoredTableStateType> {
        match self {
            Self::Init => Ok(StoredTableStateType::Init),
            Self::DataSync => Ok(StoredTableStateType::DataSync),
            Self::FinishedCopy => Ok(StoredTableStateType::FinishedCopy),
            Self::SyncDone => Ok(StoredTableStateType::SyncDone),
            Self::Ready => Ok(StoredTableStateType::Ready),
            Self::Errored => Ok(StoredTableStateType::Errored),
            Self::SyncWait | Self::Catchup => Err(etl_error!(
                ErrorKind::InvalidState,
                "In-memory table state cannot be converted to storage state",
                "In-memory table states (SyncWait, Catchup) cannot be saved to state store"
            )),
        }
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

/// Serde helpers for schema snapshot identifiers.
mod snapshot_id_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::schema::SnapshotId;

    pub(super) fn serialize<S>(snapshot_id: &SnapshotId, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        snapshot_id.to_string().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<SnapshotId, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse::<SnapshotId>().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use etl_postgres::store::table_state;
    use tokio_postgres::types::PgLsn;

    use crate::{
        error::ErrorKind,
        etl_error,
        replication::state::{
            StoredTableDecodingState, TableRetryPolicy, TableState, TableStateType,
        },
        schema::{SnapshotId, TableId, TableName, TableSchema},
    };

    /// Builds an empty table schema at an exact snapshot for decoder tests.
    fn table_schema_at(snapshot_id: SnapshotId) -> Arc<TableSchema> {
        Arc::new(TableSchema::with_snapshot_id(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![],
            snapshot_id,
        ))
    }

    /// Builds a stored decoder at an exact snapshot with empty masks.
    fn decoding_state_at(snapshot_id: SnapshotId) -> StoredTableDecodingState {
        StoredTableDecodingState { snapshot_id, replication_mask: vec![], identity_mask: vec![] }
    }

    #[test]
    fn materialize_excludes_replayable_transaction_at_sync_done() {
        for frontier in [1, 100, u64::MAX] {
            let sync_done_lsn = PgLsn::from(frontier);
            let previous_snapshot =
                SnapshotId::new(PgLsn::from(frontier - 1), PgLsn::from(u64::MAX));
            decoding_state_at(previous_snapshot)
                .materialize(table_schema_at(previous_snapshot), sync_done_lsn)
                .unwrap();

            for message_lsn in [0, 50, u64::MAX] {
                let replayed_snapshot = SnapshotId::new(sync_done_lsn, PgLsn::from(message_lsn));
                let error = decoding_state_at(replayed_snapshot)
                    .materialize(table_schema_at(replayed_snapshot), sync_done_lsn)
                    .unwrap_err();
                assert_eq!(error.kind(), ErrorKind::InvalidState);
            }
        }

        decoding_state_at(SnapshotId::initial())
            .materialize(table_schema_at(SnapshotId::initial()), PgLsn::from(0))
            .unwrap();
    }

    #[test]
    fn table_state_json_round_trip() {
        let init = TableState::Init;
        let json = serde_json::to_value(&init).unwrap();
        assert_eq!(json, serde_json::json!({"type": "init"}));
        let deserialized: TableState = serde_json::from_value(json).unwrap();
        assert!(matches!(deserialized, TableState::Init));

        let lsn = "0/1000000".parse::<PgLsn>().unwrap();
        let sync_done = TableState::SyncDone { lsn, table_decoding_state: None };
        let json = serde_json::to_value(&sync_done).unwrap();
        assert_eq!(json, serde_json::json!({"type": "sync_done", "lsn": "0/1000000"}));
        let deserialized: TableState = serde_json::from_value(json).unwrap();
        if let TableState::SyncDone { lsn: got, table_decoding_state: None } = deserialized {
            assert_eq!(got, lsn);
        } else {
            panic!("Expected SyncDone variant");
        }

        let table_decoding_state = StoredTableDecodingState {
            snapshot_id: SnapshotId::new(
                "0/800000".parse::<PgLsn>().unwrap(),
                "0/900000".parse::<PgLsn>().unwrap(),
            ),
            replication_mask: vec![1, 0, 1],
            identity_mask: vec![1, 0, 0],
        };
        let sync_done =
            TableState::SyncDone { lsn, table_decoding_state: Some(table_decoding_state) };
        let json = serde_json::to_value(&sync_done).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "type": "sync_done",
                "lsn": "0/1000000",
                "table_decoding_state": {
                    "snapshot_id": "8388608:9437184",
                    "replication_mask": [1, 0, 1],
                    "identity_mask": [1, 0, 0]
                }
            })
        );
        let deserialized: TableState = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized, sync_done);

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
    fn sync_done_json_rejects_incomplete_decoding_state() {
        let incomplete_decoding_state = serde_json::json!({
            "type": "sync_done",
            "lsn": "0/1000000",
            "table_decoding_state": {
                "snapshot_id": "8388608:9437184",
                "replication_mask": [1, 0, 1]
            }
        });

        assert!(serde_json::from_value::<TableState>(incomplete_decoding_state).is_err());
    }

    #[test]
    fn sync_done_json_rejects_legacy_message_lsn_snapshot_id() {
        let legacy_state = serde_json::json!({
            "type": "sync_done",
            "lsn": "0/1000000",
            "table_decoding_state": {
                "snapshot_id": "0/900000",
                "replication_mask": [1, 0, 1],
                "identity_mask": [1, 0, 0]
            }
        });

        assert!(serde_json::from_value::<TableState>(legacy_state).is_err());
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
            TableStateType::Ready.to_storage_type().unwrap();

        assert_eq!(state_type, table_state::StoredTableStateType::Ready);
    }

    #[test]
    fn state_type_classifies_sync_lifecycle() {
        let syncing_states = [
            TableStateType::Init,
            TableStateType::DataSync,
            TableStateType::FinishedCopy,
            TableStateType::SyncWait,
            TableStateType::Catchup,
            TableStateType::SyncDone,
        ];
        assert!(syncing_states.iter().all(TableStateType::is_syncing));

        let non_syncing_states = [TableStateType::Ready, TableStateType::Errored];
        assert!(non_syncing_states.iter().all(|state| !state.is_syncing()));

        let states_without_completed_sync = [
            TableStateType::Init,
            TableStateType::DataSync,
            TableStateType::FinishedCopy,
            TableStateType::SyncWait,
            TableStateType::Catchup,
            TableStateType::Errored,
        ];
        assert!(
            states_without_completed_sync.iter().all(|state| !state.has_completed_table_sync())
        );

        let completed_states = [TableStateType::SyncDone, TableStateType::Ready];
        assert!(completed_states.iter().all(TableStateType::has_completed_table_sync));

        let states_that_repeat_sync = [
            TableStateType::Init,
            TableStateType::DataSync,
            TableStateType::FinishedCopy,
            TableStateType::SyncWait,
            TableStateType::Catchup,
        ];
        assert!(states_that_repeat_sync.iter().all(TableStateType::would_perform_table_sync));

        let states_that_do_not_repeat_sync =
            [TableStateType::SyncDone, TableStateType::Ready, TableStateType::Errored];
        assert!(
            states_that_do_not_repeat_sync.iter().all(|state| !state.would_perform_table_sync())
        );
    }

    #[test]
    fn from_state_row_fails_on_missing_metadata() {
        let row = table_state::StoredTableStateRow {
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
        let row = table_state::StoredTableStateRow {
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
        let sync_done_lsn = "0/1000000".parse::<PgLsn>().unwrap();
        let table_decoding_state = StoredTableDecodingState {
            snapshot_id: SnapshotId::new(
                "0/800000".parse::<PgLsn>().unwrap(),
                "0/900000".parse::<PgLsn>().unwrap(),
            ),
            replication_mask: vec![1, 0, 1],
            identity_mask: vec![1, 0, 0],
        };
        let states = [
            TableState::Init,
            TableState::DataSync,
            TableState::FinishedCopy,
            // Keep the missing payload covered for rows written before durable table decoding
            // state was added to SyncDone.
            TableState::SyncDone { lsn: sync_done_lsn, table_decoding_state: None },
            // New SyncDone rows must preserve the complete compact decoder.
            TableState::SyncDone {
                lsn: sync_done_lsn,
                table_decoding_state: Some(table_decoding_state),
            },
            TableState::Ready,
            TableState::Errored {
                reason: "broken".to_owned(),
                solution: Some("fix it".to_owned()),
                retry_policy: TableRetryPolicy::ManualRetry,
                source_err: etl_error!(ErrorKind::Unknown, "Test"),
            },
        ];

        for expected_state in states {
            let expected_state_type = expected_state.as_type().to_storage_type().unwrap();
            let (stored_state_type, metadata) = expected_state.to_storage_format().unwrap();
            assert_eq!(stored_state_type, expected_state_type);

            let row = table_state::StoredTableStateRow {
                id: 1,
                pipeline_id: 1,
                table_id: sqlx::postgres::types::Oid(42),
                state: stored_state_type,
                metadata: Some(metadata),
                prev: None,
                is_current: true,
            };
            let restored_state = TableState::from_state_row(row).unwrap();

            assert_eq!(restored_state, expected_state);
        }
    }

    /// Generative checks that durable table states survive storage and that
    /// stored decoders rebuild exactly the schema they were captured from.
    #[cfg(feature = "test-utils")]
    mod durable_state_properties {
        use std::sync::Arc;

        use chrono::DateTime;
        use etl_postgres::store::table_state;
        use proptest::{
            arbitrary::any,
            bool::weighted,
            collection::vec,
            option, prop_assert, prop_assert_eq, prop_oneof,
            strategy::{Just, Strategy},
            test_runner::TestCaseError,
        };
        use tokio_postgres::types::{PgLsn, Type};

        use crate::{
            error::ErrorKind,
            etl_error,
            replication::state::{StoredTableDecodingState, TableRetryPolicy, TableState},
            schema::{
                ColumnSchema, IdentityMask, IdentityType, ReplicatedTableSchema, ReplicationMask,
                SnapshotId, TableId, TableName, TableSchema,
            },
            test_utils::property::run_property,
        };

        /// Upper bound on generated table width.
        const MAX_COLUMNS: usize = 10;

        /// Last second of year 9999, the latest retry timestamp generated.
        ///
        /// Retry timestamps come from `now + configured delay`, so later years
        /// are outside what the pipeline can write.
        const MAX_RETRY_TIMESTAMP_SECS: i64 = 253_402_300_799;

        /// Generates LSNs biased toward the encoding boundaries.
        fn lsn() -> impl Strategy<Value = PgLsn> {
            prop_oneof![
                Just(0_u64),
                Just(u64::MAX),
                Just(u64::from(u32::MAX)),
                Just(u64::from(u32::MAX) + 1),
                any::<u64>(),
            ]
            .prop_map(PgLsn::from)
        }

        /// Generates schema snapshot identifiers.
        fn snapshot_id() -> impl Strategy<Value = SnapshotId> {
            (lsn(), lsn())
                .prop_map(|(commit_lsn, message_lsn)| SnapshotId::new(commit_lsn, message_lsn))
        }

        /// Generates every retry policy the pipeline can persist.
        fn retry_policy() -> impl Strategy<Value = TableRetryPolicy> {
            prop_oneof![
                Just(TableRetryPolicy::NoRetry),
                Just(TableRetryPolicy::ManualRetry),
                (0..=MAX_RETRY_TIMESTAMP_SECS, 0..1_000_000_000_u32).prop_map(|(secs, nanos)| {
                    TableRetryPolicy::TimedRetry {
                        next_retry: DateTime::from_timestamp(secs, nanos).unwrap(),
                    }
                }),
            ]
        }

        /// Generates every durable [`TableState`] variant.
        ///
        /// Stored masks are arbitrary bytes because storage must preserve
        /// them exactly; validation belongs to
        /// [`StoredTableDecodingState::materialize`].
        fn durable_table_state() -> impl Strategy<Value = TableState> {
            let table_decoding_state = (
                snapshot_id(),
                vec(any::<u8>(), 0..=MAX_COLUMNS),
                vec(any::<u8>(), 0..=MAX_COLUMNS),
            )
                .prop_map(|(snapshot_id, replication_mask, identity_mask)| {
                    StoredTableDecodingState { snapshot_id, replication_mask, identity_mask }
                });
            prop_oneof![
                Just(TableState::Init),
                Just(TableState::DataSync),
                Just(TableState::FinishedCopy),
                (lsn(), option::of(table_decoding_state)).prop_map(
                    |(lsn, table_decoding_state)| TableState::SyncDone {
                        lsn,
                        table_decoding_state
                    }
                ),
                Just(TableState::Ready),
                (any::<String>(), option::of(any::<String>()), retry_policy()).prop_map(
                    |(reason, solution, retry_policy)| TableState::Errored {
                        reason,
                        solution,
                        retry_policy,
                        source_err: etl_error!(ErrorKind::Unknown, "Generated error"),
                    }
                ),
            ]
        }

        /// Writes a state in storage format and reads it back as a state row.
        fn store_and_load(state: &TableState) -> Result<TableState, TestCaseError> {
            let (stored_state_type, metadata) = state.to_storage_format().unwrap();
            prop_assert_eq!(stored_state_type, state.as_type().to_storage_type().unwrap());

            let row = table_state::StoredTableStateRow {
                id: 1,
                pipeline_id: 1,
                table_id: sqlx::postgres::types::Oid(42),
                state: stored_state_type,
                metadata: Some(metadata),
                prev: None,
                is_current: true,
            };

            Ok(TableState::from_state_row(row).unwrap())
        }

        /// A replicated schema plus the `SyncDone` LSN that captured it.
        #[derive(Debug, Clone)]
        struct CapturedDecoder {
            /// Primary-key membership of each column.
            primary_key: Vec<bool>,
            /// Publication-column membership.
            replicated: Vec<bool>,
            /// Replica-identity membership before restricting it to
            /// replicated columns.
            identity: Vec<bool>,
            /// Commit LSN of the captured snapshot.
            snapshot_commit_lsn: u64,
            /// Message LSN of the captured snapshot.
            snapshot_message_lsn: u64,
            /// Distance from the snapshot commit to `SyncDone`.
            sync_done_distance: u64,
        }

        /// Generates a valid captured decoder.
        fn captured_decoder() -> impl Strategy<Value = CapturedDecoder> {
            (0..=MAX_COLUMNS)
                .prop_flat_map(|column_count| {
                    (
                        vec(weighted(0.3), column_count),
                        vec(weighted(0.8), column_count),
                        vec(weighted(0.4), column_count),
                        0..u64::MAX - 1,
                        any::<u64>(),
                        prop_oneof![Just(1_u64), Just(u64::MAX), any::<u64>()],
                    )
                })
                .prop_map(
                    |(
                        primary_key,
                        replicated,
                        identity,
                        snapshot_commit_lsn,
                        snapshot_message_lsn,
                        sync_done_distance,
                    )| CapturedDecoder {
                        primary_key,
                        replicated,
                        identity,
                        snapshot_commit_lsn,
                        snapshot_message_lsn,
                        sync_done_distance,
                    },
                )
        }

        /// Builds the stored table schema of a captured decoder.
        fn captured_table_schema(decoder: &CapturedDecoder) -> Arc<TableSchema> {
            let snapshot_id = SnapshotId::new(
                PgLsn::from(decoder.snapshot_commit_lsn),
                PgLsn::from(decoder.snapshot_message_lsn),
            );
            let mut primary_key_ordinal = 0;
            let column_schemas = decoder
                .primary_key
                .iter()
                .enumerate()
                .map(|(index, &primary_key)| {
                    let attnum = i32::try_from(index + 1).unwrap();
                    let column =
                        ColumnSchema::new(format!("c{index}"), Type::INT4, -1, attnum, true);
                    if primary_key {
                        primary_key_ordinal += 1;
                        column.with_primary_key(primary_key_ordinal)
                    } else {
                        column
                    }
                })
                .collect();

            Arc::new(TableSchema::with_snapshot_id(
                TableId::new(1),
                TableName::new("public".to_owned(), "t".to_owned()),
                column_schemas,
                snapshot_id,
            ))
        }

        /// Returns the first `SyncDone` LSN whose frontier still contains the
        /// captured snapshot, moved forward by the generated distance.
        fn sync_done_lsn(decoder: &CapturedDecoder) -> PgLsn {
            let earliest = decoder.snapshot_commit_lsn + 1;
            PgLsn::from(earliest.saturating_add(decoder.sync_done_distance.saturating_sub(1)))
        }

        /// Projects a replicated schema onto everything row decoding uses.
        fn decoder_view(
            schema: &ReplicatedTableSchema,
        ) -> (Vec<u8>, Vec<u8>, Vec<String>, Vec<String>, IdentityType) {
            (
                schema.replication_mask().to_bytes(),
                schema.identity_mask().to_bytes(),
                schema.column_schemas().map(|column| column.name.clone()).collect(),
                schema.identity_column_schemas().map(|column| column.name.clone()).collect(),
                schema.identity_type(),
            )
        }

        #[test]
        fn durable_table_states_round_trip_through_storage() {
            run_property("durable table state round trip", &durable_table_state(), |state| {
                let restored = store_and_load(state)?;
                prop_assert_eq!(&restored, state);
                Ok(())
            });
        }

        #[test]
        fn stored_sync_done_decoder_restores_the_captured_schema() {
            run_property("sync done decoder restore", &captured_decoder(), |decoder| {
                let table_schema = captured_table_schema(decoder);
                let replication_mask =
                    decoder.replicated.iter().map(|&replicated| u8::from(replicated)).collect();
                let identity_mask = decoder
                    .replicated
                    .iter()
                    .zip(&decoder.identity)
                    .map(|(&replicated, &identity)| u8::from(replicated && identity))
                    .collect();
                let captured = ReplicatedTableSchema::from_masks(
                    Arc::clone(&table_schema),
                    ReplicationMask::from_bytes(replication_mask),
                    IdentityMask::from_bytes(identity_mask),
                );
                let sync_done_lsn = sync_done_lsn(decoder);

                let restored_state =
                    store_and_load(&TableState::sync_done(sync_done_lsn, &captured))?;
                let TableState::SyncDone { lsn, table_decoding_state: Some(table_decoding_state) } =
                    restored_state
                else {
                    return Err(TestCaseError::fail("SyncDone lost its decoding state"));
                };
                prop_assert_eq!(lsn, sync_done_lsn);
                prop_assert_eq!(table_decoding_state.snapshot_id(), table_schema.snapshot_id);

                let restored = table_decoding_state.materialize(table_schema, lsn).unwrap();
                prop_assert_eq!(decoder_view(&restored), decoder_view(&captured));
                Ok(())
            });
        }

        #[test]
        fn stored_decoder_materializes_only_valid_states() {
            let strategy = (
                0..=MAX_COLUMNS,
                vec(prop_oneof![8 => 0_u8..=1, 1 => any::<u8>()], 0..=MAX_COLUMNS + 1),
                vec(prop_oneof![8 => 0_u8..=1, 1 => any::<u8>()], 0..=MAX_COLUMNS + 1),
                0_u64..=4,
                0_u64..=4,
            );
            run_property(
                "stored decoder validation",
                &strategy,
                |(column_count, replication_mask, identity_mask, snapshot_commit, sync_done)| {
                    let decoder = CapturedDecoder {
                        primary_key: vec![false; *column_count],
                        replicated: vec![],
                        identity: vec![],
                        snapshot_commit_lsn: *snapshot_commit,
                        snapshot_message_lsn: 0,
                        sync_done_distance: 1,
                    };
                    let table_schema = captured_table_schema(&decoder);
                    let stored = StoredTableDecodingState {
                        snapshot_id: table_schema.snapshot_id,
                        replication_mask: replication_mask.clone(),
                        identity_mask: identity_mask.clone(),
                    };

                    // A transaction committing at `SyncDone` belongs to the
                    // apply worker, so only strictly earlier snapshots are
                    // part of the handover decoder. A zero `SyncDone` still
                    // admits the initial `0/0` snapshot sentinel.
                    let snapshot_reachable =
                        snapshot_commit < sync_done || (*snapshot_commit == 0 && *sync_done == 0);
                    let valid = snapshot_reachable
                        && replication_mask.len() == *column_count
                        && identity_mask.len() == *column_count
                        && replication_mask.iter().chain(identity_mask).all(|value| *value <= 1)
                        && replication_mask
                            .iter()
                            .zip(identity_mask)
                            .all(|(replicated, identity)| identity <= replicated);
                    let result = stored.materialize(table_schema, PgLsn::from(*sync_done));
                    prop_assert_eq!(result.is_ok(), valid);
                    if let Ok(schema) = result {
                        prop_assert_eq!(
                            schema.replication_mask().as_slice(),
                            &replication_mask[..]
                        );
                        prop_assert_eq!(schema.identity_mask().as_slice(), &identity_mask[..]);
                        prop_assert!(
                            schema.column_schemas().len() == schema.column_schemas().count()
                        );
                    }
                    Ok(())
                },
            );
        }
    }
}
