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
//! materialize while retaining any previous relation masks. PostgreSQL emits a
//! new relation before row data when its pgoutput relation state changed;
//! otherwise ETL combines the retained masks with the new stored table schema
//! and transitions back to `WithSchema`. Any complete decoder can be converted
//! into the compact
//! [`crate::replication::state::StoredTableDecodingState`] persisted in
//! `SyncDone`. Reaching the ownership boundary while still pending fails closed
//! because relation-less DML has not yet confirmed that the previous masks are
//! still authoritative.
//!
//! The apply worker clears its entry before starting table synchronization.
//! After `SyncDone`, an owned relation materializes a new entry, while
//! relation-less DML restores the compact durable state on demand.
//! Consequently, `WithSchema` also proves that the current apply connection can
//! keep decoding after `SyncDone` is replaced by `Ready`.

use std::sync::Arc;

use crate::schema::{
    IdentityMask, ReplicatedTableSchema, ReplicationMask, SnapshotId, TableSchema,
};

/// Complete relation masks retained across a DDL message.
#[derive(Debug, Clone)]
pub(crate) struct PreviousRelationMasks {
    /// Publication-column membership from the previous relation.
    replication_mask: ReplicationMask,
    /// Replica-identity membership from the previous relation.
    identity_mask: IdentityMask,
}

impl PreviousRelationMasks {
    /// Captures both masks from a materialized decoder.
    fn from_schema(schema: &ReplicatedTableSchema) -> Self {
        Self {
            replication_mask: schema.replication_mask().clone(),
            identity_mask: schema.identity_mask().clone(),
        }
    }

    /// Combines the retained masks with the actual new table schema.
    pub(crate) fn materialize(self, table_schema: Arc<TableSchema>) -> ReplicatedTableSchema {
        ReplicatedTableSchema::from_masks(table_schema, self.replication_mask, self.identity_mask)
    }
}

/// Per-table row-decoding state for one logical replication connection.
#[derive(Debug, Clone)]
pub(crate) enum TableDecodingState {
    /// A DDL message was observed and the next relation belongs to its schema
    /// snapshot.
    PendingRelation {
        /// Exact schema snapshot that the next relation must materialize.
        snapshot_id: SnapshotId,
        /// Complete masks from the previous pgoutput relation, if known.
        previous_relation_masks: Option<PreviousRelationMasks>,
    },
    /// Complete row-decoding state materialized from a relation or restored
    /// `SyncDone` decoding state.
    WithSchema(ReplicatedTableSchema),
}

impl TableDecodingState {
    /// Records a pending relation while retaining masks from the previous
    /// pgoutput relation.
    pub(crate) fn pending_relation(snapshot_id: SnapshotId, previous: Option<Self>) -> Self {
        let previous_relation_masks = match previous {
            Some(Self::PendingRelation { previous_relation_masks, .. }) => previous_relation_masks,
            Some(Self::WithSchema(schema)) => Some(PreviousRelationMasks::from_schema(&schema)),
            None => None,
        };

        Self::PendingRelation { snapshot_id, previous_relation_masks }
    }
}
