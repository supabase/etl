//! Connection-local per-table decoding state for logical replication workers.
//!
//! PostgreSQL 15+ supports column-level publication filtering, where only
//! specific columns are replicated rather than all columns. Each logical
//! replication connection therefore needs three pieces of protocol state to
//! decode row changes: the schema snapshot to decode against, the publication
//! column filter for that snapshot, and the replica-identity semantics for
//! that same snapshot.
//!
//! The connection-local map has two states:
//!
//! - [`TableDecodingState::WithSchema`] is a complete decoder and can decode a
//!   row immediately.
//! - [`TableDecodingState::PendingRelation`] records a DDL snapshot while ETL
//!   waits to see whether pgoutput sends replacement relation metadata. When a
//!   previous complete decoder exists, the pending state retains both of its
//!   relation masks as a fallback. A new `RELATION` replaces those masks. If a
//!   row arrives first, ETL combines the fallback masks with the stored schema
//!   at the pending snapshot and transitions to `WithSchema`. Without locally
//!   retained fallback masks, the apply worker can still resolve them from an
//!   applicable complete `SyncDone` decoder; otherwise rows cannot be decoded
//!   until a `RELATION` arrives.
//!
//! An absent map entry is not another [`TableDecodingState`]: it means this
//! connection has not established any state for the table. After table-sync
//! handover, the apply worker may restore that missing state from the compact
//! [`crate::replication::state::StoredTableDecodingState`] persisted in
//! `SyncDone`.
//!
//! A table-sync worker seeds `WithSchema` from its consistent initial-copy
//! snapshot. The apply worker clears its entry before starting table sync, then
//! establishes `WithSchema` from an owned relation or durable `SyncDone` state
//! after handover. Consequently, `WithSchema` also proves that the current
//! apply connection can keep decoding after `SyncDone` becomes `Ready`.

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
    pub(super) fn from_schema(schema: &ReplicatedTableSchema) -> Self {
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
    /// A DDL message was observed, but pgoutput has not yet shown whether it
    /// will send replacement relation metadata before the next row.
    PendingRelation {
        /// Exact stored schema snapshot for the DDL message.
        snapshot_id: SnapshotId,
        /// Complete masks from the previous decoder, retained only as a
        /// relation-less row fallback.
        previous_relation_masks: Option<PreviousRelationMasks>,
    },
    /// Complete row-decoding state ready for immediate use.
    WithSchema(ReplicatedTableSchema),
}

impl TableDecodingState {
    /// Records a pending relation while retaining masks from the previous
    /// complete decoder.
    pub(crate) fn pending_relation(snapshot_id: SnapshotId, previous: Option<Self>) -> Self {
        let previous_relation_masks = match previous {
            Some(Self::PendingRelation { previous_relation_masks, .. }) => previous_relation_masks,
            Some(Self::WithSchema(schema)) => Some(PreviousRelationMasks::from_schema(&schema)),
            None => None,
        };

        Self::PendingRelation { snapshot_id, previous_relation_masks }
    }
}
