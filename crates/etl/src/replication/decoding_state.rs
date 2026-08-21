//! Connection-local per-table decoding state for logical replication workers.
//!
//! PostgreSQL 15+ supports column-level publication filtering, where only
//! specific columns are replicated rather than all columns. Each logical
//! replication connection therefore needs three pieces of protocol state to
//! decode row changes: the schema snapshot to decode against, the publication
//! column filter for that snapshot, and the replica-identity semantics for
//! that same snapshot.
//!
//! A table-sync worker seeds this state from its consistent initial-copy
//! snapshot and updates it whenever PostgreSQL emits a new `RELATION` message.
//! A DDL message records the exact snapshot that the next relation must
//! materialize while retaining any previous decoder. PostgreSQL emits a new
//! relation before row data when its pgoutput relation state changed; otherwise
//! the previous decoder remains valid for relation-less rows. Any complete
//! decoder can be converted into the compact
//! [`crate::replication::state::StoredTableDecodingState`] persisted in
//! `SyncDone`. Reaching the ownership boundary without either a new relation or
//! a previous decoder fails closed because the publication and replica-identity
//! masks are not known.
//!
//! The apply worker clears its entry before starting table synchronization.
//! After `SyncDone`, an owned relation materializes a new entry, while
//! relation-less DML restores the compact durable state on demand.
//! Consequently, `WithSchema` also proves that the current apply connection can
//! keep decoding after `SyncDone` is replaced by `Ready`.

use crate::schema::{ReplicatedTableSchema, SnapshotId};

/// Per-table row-decoding state for one logical replication connection.
#[derive(Debug, Clone)]
pub(crate) enum TableDecodingState {
    /// A DDL message was observed and the next relation belongs to its schema
    /// snapshot.
    PendingRelation {
        /// Exact schema snapshot that the next relation must materialize.
        snapshot_id: SnapshotId,
        /// Previous complete decoder used when PostgreSQL emits row data
        /// without a new relation.
        previous_schema: Option<ReplicatedTableSchema>,
    },
    /// Complete row-decoding state materialized from a relation or restored
    /// `SyncDone` decoding state.
    WithSchema(ReplicatedTableSchema),
}

impl TableDecodingState {
    /// Records a pending relation while retaining any complete decoder from
    /// the previous state.
    pub(crate) fn pending_relation(snapshot_id: SnapshotId, previous: Option<Self>) -> Self {
        let previous_schema = match previous {
            Some(Self::PendingRelation { previous_schema, .. }) => previous_schema,
            Some(Self::WithSchema(schema)) => Some(schema),
            None => None,
        };

        Self::PendingRelation { snapshot_id, previous_schema }
    }

    /// Returns the complete decoder available for relation-less row data.
    pub(crate) fn schema_for_row(&self) -> Option<&ReplicatedTableSchema> {
        match self {
            Self::PendingRelation { previous_schema, .. } => previous_schema.as_ref(),
            Self::WithSchema(schema) => Some(schema),
        }
    }
}
