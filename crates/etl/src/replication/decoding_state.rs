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
//! A DDL message moves the table into `WaitingForRelation` with the exact
//! snapshot that the next relation must materialize. Only a complete
//! `WithSchema` state can be converted into the compact
//! [`crate::replication::state::StoredTableDecodingState`] persisted in
//! `SyncDone`. Reaching the ownership boundary while still waiting for a
//! relation fails closed because the publication and replica-identity masks
//! are not known.
//!
//! The apply worker clears its entry before starting table synchronization.
//! After `SyncDone`, an owned relation materializes a new entry, while
//! relation-less DML restores the compact durable state. Consequently,
//! `WithSchema` also proves that the current apply connection can keep decoding
//! after `SyncDone` is replaced by `Ready`.

use crate::schema::{ReplicatedTableSchema, SnapshotId};

/// Per-table row-decoding state for one logical replication connection.
#[derive(Debug, Clone)]
pub(crate) enum TableDecodingState {
    /// A DDL message was observed and a new relation is required before rows
    /// can be decoded.
    WaitingForRelation {
        /// Exact schema snapshot that the next relation must materialize.
        snapshot_id: SnapshotId,
    },
    /// Complete row-decoding state materialized from a relation or restored
    /// `SyncDone` decoding state.
    WithSchema(ReplicatedTableSchema),
}
