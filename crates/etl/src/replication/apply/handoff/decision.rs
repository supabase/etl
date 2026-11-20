//! Pure table-sync handoff decisions.
//!
//! [`decide`] maps one observation of a syncing table to a [`HandoffAction`]
//! without taking a lock, reading a store, or doing I/O.
//! [`crate::replication::apply::handoff`] documents the transition table it
//! implements and executes its answers.

use tokio_postgres::types::PgLsn;

use crate::replication::state::TableState;

/// Point at which the apply worker evaluates syncing tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Trigger {
    /// A COMMIT was processed.
    AfterCommit,
    /// A destination flush became durable.
    AfterFlush,
    /// The apply loop is quiescent.
    Quiescent,
}

impl Trigger {
    /// Returns the trigger's log label.
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::AfterCommit => "after_commit",
            Self::AfterFlush => "after_flush",
            Self::Quiescent => "quiescent",
        }
    }
}

/// Progress that decides whether a `SyncDone` table may become `Ready`.
#[derive(Debug, Clone, Copy)]
pub(super) struct ReadinessInputs {
    /// Apply checkpoint persisted before this evaluation, if any.
    persisted_checkpoint_lsn: Option<PgLsn>,
    /// Whether this connection holds a complete decoder for the table.
    has_materialized_decoding_state: bool,
}

impl ReadinessInputs {
    /// Creates the readiness inputs for one table.
    pub(super) fn new(
        persisted_checkpoint_lsn: Option<PgLsn>,
        has_materialized_decoding_state: bool,
    ) -> Self {
        Self { persisted_checkpoint_lsn, has_materialized_decoding_state }
    }

    /// Returns the persisted apply checkpoint when `SyncDone` at
    /// `sync_done_lsn` may be stored as `Ready`.
    ///
    /// The three checks cover separate safety obligations:
    ///
    /// - `current_lsn >= sync_done_lsn` proves that this connection reached the
    ///   boundary where apply ownership resumes.
    /// - `persisted_checkpoint_lsn >= sync_done_lsn` proves restart safety.
    ///   Requiring the stored value, not the apply loop's in-memory checkpoint,
    ///   makes `Ready(H) => persisted apply checkpoint >= H` hold across
    ///   crashes and checkpoint-write failures. Startup selects the later of
    ///   the replication-slot position and this checkpoint, so a later
    ///   connection has a bootstrap snapshot at or beyond `SyncDone`. A fresh
    ///   pgoutput connection emits relation metadata before its first row
    ///   change, so that relation can resolve the newest stored schema at or
    ///   before the bootstrap.
    /// - `has_materialized_decoding_state` proves current-connection safety.
    ///   PostgreSQL may have sent the relation before `SyncDone`, while table
    ///   sync still owned the table. Apply skipped that relation, and
    ///   PostgreSQL need not send it again on the same connection. If DML then
    ///   arrives without a local decoder, apply can recover only while
    ///   `SyncDone` still retains the exact snapshot and masks.
    ///
    /// Apply-side decoding state is cleared before table sync starts.
    /// Consequently, a later
    /// [`crate::replication::TableDecodingState::WithSchema`] proves that this
    /// connection now has a complete decoder: relation-less DML restored it
    /// from `SyncDone`, or an owned relation materialized it. Only then may
    /// `Ready` discard the durable fallback.
    ///
    /// The decoder check does not make a table that is already `Ready` safer
    /// across restart; the persisted-checkpoint condition provides that. A
    /// restarted loop which still finds `SyncDone` applies this same check
    /// only because it is now the current connection whose decoder must be
    /// established before it stores `Ready`.
    ///
    /// A handled DDL records a pending relation while retaining any previous
    /// masks. PostgreSQL's following relation materializes the new exact
    /// snapshot and masks. A relation-less row instead combines the retained
    /// masks with the stored new table schema and transitions to `WithSchema`.
    ///
    /// A table which lacks local decoding state remains in `SyncDone`; this
    /// does not block owned changes at or after the boundary. Its first
    /// relation or DML can materialize the decoder on demand.
    pub(super) fn ready_checkpoint(
        &self,
        current_lsn: PgLsn,
        sync_done_lsn: PgLsn,
    ) -> Option<PgLsn> {
        if !self.has_materialized_decoding_state || current_lsn < sync_done_lsn {
            return None;
        }

        self.persisted_checkpoint_lsn.filter(|lsn| *lsn >= sync_done_lsn)
    }
}

/// Action the apply worker should take for one syncing table.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum HandoffAction {
    /// Move `SyncWait` to `Catchup` at `catchup_lsn`, then wait for `SyncDone`.
    StartCatchup {
        /// LSN of the snapshot the table was copied from.
        snapshot_lsn: PgLsn,
        /// LSN this connection had reached.
        current_lsn: PgLsn,
        /// LSN the table-sync worker must stream up to.
        catchup_lsn: PgLsn,
    },
    /// Wait for a worker already in `Catchup` to reach `SyncDone`.
    AwaitCatchup {
        /// LSN the table-sync worker is streaming up to.
        catchup_lsn: PgLsn,
    },
    /// Store `Ready`.
    PromoteToReady {
        /// `SyncDone` boundary the readiness checks covered.
        sync_done_lsn: PgLsn,
        /// LSN this connection had reached.
        current_lsn: PgLsn,
        /// Persisted apply checkpoint, at or beyond `sync_done_lsn`.
        persisted_checkpoint_lsn: PgLsn,
    },
    /// Start a table-sync worker for the table.
    SpawnWorker,
    /// Leave the table unchanged.
    Keep,
}

/// Decides the handoff action for one syncing table.
///
/// Mirrors the transition table in [`crate::replication::apply::handoff`].
/// `SyncWait` enters catchup at the later of the copy snapshot and
/// `current_lsn`, because the worker may have started from a position ahead of
/// this loop.
pub(super) fn decide(
    trigger: Trigger,
    worker_running: bool,
    state: &TableState,
    current_lsn: PgLsn,
    readiness: Option<&ReadinessInputs>,
) -> HandoffAction {
    let promote = |sync_done_lsn: PgLsn| match readiness
        .and_then(|inputs| inputs.ready_checkpoint(current_lsn, sync_done_lsn))
    {
        Some(persisted_checkpoint_lsn) => {
            HandoffAction::PromoteToReady { sync_done_lsn, current_lsn, persisted_checkpoint_lsn }
        }
        None => HandoffAction::Keep,
    };

    match (worker_running, state, trigger) {
        (true, TableState::SyncWait { lsn }, Trigger::AfterCommit | Trigger::Quiescent) => {
            HandoffAction::StartCatchup {
                snapshot_lsn: *lsn,
                current_lsn,
                catchup_lsn: (*lsn).max(current_lsn),
            }
        }
        (true, TableState::Catchup { lsn }, Trigger::AfterCommit | Trigger::Quiescent) => {
            HandoffAction::AwaitCatchup { catchup_lsn: *lsn }
        }
        (_, TableState::SyncDone { lsn, .. }, Trigger::AfterFlush | Trigger::Quiescent) => {
            promote(*lsn)
        }
        (false, TableState::SyncDone { .. }, Trigger::AfterCommit) => HandoffAction::Keep,
        (false, _, _) => HandoffAction::SpawnWorker,
        (true, _, _) => HandoffAction::Keep,
    }
}

#[cfg(test)]
mod tests {
    use tokio_postgres::types::PgLsn;

    use crate::{
        error::ErrorKind,
        etl_error,
        replication::{
            apply::handoff::decision::{HandoffAction, ReadinessInputs, Trigger, decide},
            state::{TableRetryPolicy, TableState},
        },
    };

    /// LSNs whose pairwise orders cover every comparison the handoff makes,
    /// including the maximum value.
    const LSNS: [u64; 4] = [0, 1, 2, u64::MAX];

    /// Effect of one handoff decision, independent of logging fields.
    #[derive(Debug, PartialEq, Eq)]
    enum Effect {
        StartCatchup(PgLsn),
        AwaitCatchup(PgLsn),
        PromoteToReady(PgLsn),
        SpawnWorker,
        Keep,
    }

    impl From<HandoffAction> for Effect {
        fn from(action: HandoffAction) -> Self {
            match action {
                HandoffAction::StartCatchup { catchup_lsn, .. } => Self::StartCatchup(catchup_lsn),
                HandoffAction::AwaitCatchup { catchup_lsn } => Self::AwaitCatchup(catchup_lsn),
                HandoffAction::PromoteToReady { sync_done_lsn, .. } => {
                    Self::PromoteToReady(sync_done_lsn)
                }
                HandoffAction::SpawnWorker => Self::SpawnWorker,
                HandoffAction::Keep => Self::Keep,
            }
        }
    }

    /// Returns every table state shape, with each LSN-bearing state at every
    /// test LSN.
    fn table_states() -> Vec<TableState> {
        let mut states = vec![
            TableState::Init,
            TableState::DataSync,
            TableState::FinishedCopy,
            TableState::Ready,
            TableState::Errored {
                reason: "copy failed".to_owned(),
                solution: None,
                retry_policy: TableRetryPolicy::NoRetry,
                source_err: etl_error!(ErrorKind::Unknown, "Copy failed"),
            },
        ];
        for lsn in LSNS.map(PgLsn::from) {
            states.push(TableState::SyncWait { lsn });
            states.push(TableState::Catchup { lsn });
            states.push(TableState::SyncDone { lsn, table_decoding_state: None });
        }

        states
    }

    /// Returns the specified handoff effect.
    ///
    /// One row per trigger and worker presence, transcribed from the
    /// per-trigger handoff functions the decisions replaced; each row names its
    /// source in commit `3cd6c009` so it can be audited. The decisions must
    /// match it on every input.
    fn specified_effect(
        trigger: Trigger,
        has_active_worker: bool,
        state: &TableState,
        current_lsn: PgLsn,
        persisted_checkpoint_lsn: Option<PgLsn>,
        has_materialized_decoding_state: bool,
    ) -> Effect {
        let ready = |sync_done_lsn: PgLsn| {
            has_materialized_decoding_state
                && current_lsn >= sync_done_lsn
                && persisted_checkpoint_lsn.is_some_and(|lsn| lsn >= sync_done_lsn)
        };

        match (trigger, has_active_worker) {
            // `process_single_syncing_table_after_commit_event`, active branch.
            (Trigger::AfterCommit, true) => match state {
                TableState::SyncWait { lsn } => Effect::StartCatchup((*lsn).max(current_lsn)),
                TableState::Catchup { lsn } => Effect::AwaitCatchup(*lsn),
                _ => Effect::Keep,
            },
            // `process_single_syncing_table_after_commit_event`, no-worker branch.
            (Trigger::AfterCommit, false) => match state {
                TableState::SyncDone { .. } => Effect::Keep,
                _ => Effect::SpawnWorker,
            },
            // `process_single_syncing_table_after_flush`, active branch.
            (Trigger::AfterFlush, true) => match state {
                TableState::SyncDone { lsn, .. } if ready(*lsn) => Effect::PromoteToReady(*lsn),
                _ => Effect::Keep,
            },
            // `process_single_syncing_table_after_flush`, no-worker branch.
            (Trigger::AfterFlush, false) => match state {
                TableState::SyncDone { lsn, .. } if ready(*lsn) => Effect::PromoteToReady(*lsn),
                TableState::SyncDone { .. } => Effect::Keep,
                _ => Effect::SpawnWorker,
            },
            // `process_single_syncing_table_when_quiescent`, active branch.
            (Trigger::Quiescent, true) => match state {
                TableState::SyncWait { lsn } => Effect::StartCatchup((*lsn).max(current_lsn)),
                TableState::SyncDone { lsn, .. } if ready(*lsn) => Effect::PromoteToReady(*lsn),
                TableState::Catchup { lsn } => Effect::AwaitCatchup(*lsn),
                _ => Effect::Keep,
            },
            // `process_single_syncing_table_when_quiescent`, no-worker branch.
            (Trigger::Quiescent, false) => match state {
                TableState::SyncDone { lsn, .. } if ready(*lsn) => Effect::PromoteToReady(*lsn),
                TableState::SyncDone { .. } => Effect::Keep,
                _ => Effect::SpawnWorker,
            },
        }
    }

    #[test]
    fn decisions_match_the_handoff_specification() {
        let lsns = LSNS.map(PgLsn::from);
        let persisted_checkpoints = std::iter::once(None).chain(lsns.map(Some));

        for trigger in [Trigger::AfterCommit, Trigger::AfterFlush, Trigger::Quiescent] {
            for has_active_worker in [true, false] {
                for state in table_states() {
                    for current_lsn in lsns {
                        for persisted_checkpoint_lsn in persisted_checkpoints.clone() {
                            for has_materialized_decoding_state in [false, true] {
                                let readiness = ReadinessInputs::new(
                                    persisted_checkpoint_lsn,
                                    has_materialized_decoding_state,
                                );
                                let readiness_arg = match trigger {
                                    Trigger::AfterCommit => None,
                                    Trigger::AfterFlush | Trigger::Quiescent => Some(&readiness),
                                };

                                assert_eq!(
                                    Effect::from(decide(
                                        trigger,
                                        has_active_worker,
                                        &state,
                                        current_lsn,
                                        readiness_arg,
                                    )),
                                    specified_effect(
                                        trigger,
                                        has_active_worker,
                                        &state,
                                        current_lsn,
                                        persisted_checkpoint_lsn,
                                        has_materialized_decoding_state,
                                    ),
                                    "{trigger:?}, active worker {has_active_worker}, {state:?}, \
                                     current {current_lsn}, persisted \
                                     {persisted_checkpoint_lsn:?}, decoder \
                                     {has_materialized_decoding_state}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
