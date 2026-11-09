//! Table-sync handoff decisions for the apply worker.
//!
//! The apply worker evaluates every syncing table after a COMMIT, after a
//! durable flush, and while quiescent. Each function here decides what to do
//! from one observation of a table; the apply worker then executes that
//! decision.
//!
//! The action types encode what each trigger may do. Only a table without a
//! running worker can spawn one. The flush decision for a running worker has no
//! catchup variants, because the flush path cannot block or report a shutdown
//! exit. Promotion to `Ready` carries a [`ReadyProof`], which only
//! [`ReadinessInputs::ready_proof`] constructs, so the COMMIT decisions, which
//! receive no readiness inputs, cannot promote.

use tokio_postgres::types::PgLsn;

use crate::replication::state::TableState;

/// Progress that decides whether a `SyncDone` table may become `Ready`.
#[derive(Debug, Clone, Copy)]
pub(super) struct ReadinessInputs {
    /// LSN this connection has reached.
    current_lsn: PgLsn,
    /// Apply checkpoint persisted before this evaluation, if any.
    persisted_checkpoint_lsn: Option<PgLsn>,
    /// Whether this connection holds a complete decoder for the table.
    has_materialized_decoding_state: bool,
}

impl ReadinessInputs {
    /// Creates the readiness inputs for one table.
    pub(super) fn new(
        current_lsn: PgLsn,
        persisted_checkpoint_lsn: Option<PgLsn>,
        has_materialized_decoding_state: bool,
    ) -> Self {
        Self { current_lsn, persisted_checkpoint_lsn, has_materialized_decoding_state }
    }

    /// Returns a proof that `SyncDone` at `sync_done_lsn` may be stored as
    /// `Ready`.
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
    pub(super) fn ready_proof(&self, sync_done_lsn: PgLsn) -> Option<ReadyProof> {
        let ready = self.has_materialized_decoding_state
            && self.current_lsn >= sync_done_lsn
            && self.persisted_checkpoint_lsn.is_some_and(|lsn| lsn >= sync_done_lsn);

        ready.then_some(ReadyProof { sync_done_lsn })
    }
}

/// Evidence that a `SyncDone` table may be stored as `Ready`.
///
/// Only [`ReadinessInputs::ready_proof`] constructs it.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ReadyProof {
    /// `SyncDone` boundary the readiness checks were evaluated against.
    sync_done_lsn: PgLsn,
}

impl ReadyProof {
    /// Returns the `SyncDone` boundary the readiness checks covered.
    pub(super) fn sync_done_lsn(&self) -> PgLsn {
        self.sync_done_lsn
    }
}

/// Action for a syncing table whose sync worker is running.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum ActiveAction {
    /// Move `SyncWait` to `Catchup` at this LSN, then wait for `SyncDone`.
    StartCatchup {
        /// LSN the table-sync worker must stream up to.
        catchup_lsn: PgLsn,
    },
    /// Wait for a worker already in `Catchup` to reach `SyncDone`.
    AwaitCatchup {
        /// LSN the table-sync worker is streaming up to.
        catchup_lsn: PgLsn,
    },
    /// Store `Ready` under the worker's state lock.
    PromoteToReady(ReadyProof),
    /// Leave the table to its worker.
    Keep,
}

/// Action after a durable flush for a table whose sync worker is running.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum ActiveFlushAction {
    /// Store `Ready` under the worker's state lock.
    PromoteToReady(ReadyProof),
    /// Leave the table to its worker.
    Keep,
}

impl From<ActiveFlushAction> for ActiveAction {
    fn from(action: ActiveFlushAction) -> Self {
        match action {
            ActiveFlushAction::PromoteToReady(proof) => Self::PromoteToReady(proof),
            ActiveFlushAction::Keep => Self::Keep,
        }
    }
}

/// Action for a syncing table without a running sync worker.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum IdleAction {
    /// Store `Ready`.
    PromoteToReady(ReadyProof),
    /// Start a table-sync worker for the table.
    SpawnWorker,
    /// Leave the table unchanged.
    Keep,
}

/// Decides the action after a COMMIT for a table whose worker is running.
///
/// `SyncWait` enters catchup at the later of the copy snapshot and the commit,
/// because the worker may have started from a position ahead of this loop.
pub(super) fn active_after_commit(state: &TableState, current_lsn: PgLsn) -> ActiveAction {
    match state {
        TableState::SyncWait { lsn } => {
            ActiveAction::StartCatchup { catchup_lsn: (*lsn).max(current_lsn) }
        }
        TableState::Catchup { lsn } => ActiveAction::AwaitCatchup { catchup_lsn: *lsn },
        _ => ActiveAction::Keep,
    }
}

/// Decides the action after a durable flush for a table whose worker is
/// running.
pub(super) fn active_after_flush(
    state: &TableState,
    readiness: &ReadinessInputs,
) -> ActiveFlushAction {
    match state {
        TableState::SyncDone { lsn, .. } => readiness
            .ready_proof(*lsn)
            .map_or(ActiveFlushAction::Keep, ActiveFlushAction::PromoteToReady),
        _ => ActiveFlushAction::Keep,
    }
}

/// Decides the action while quiescent for a table whose worker is running.
///
/// A quiescent loop does the COMMIT work for the table, or the flush work when
/// the COMMIT decision keeps it.
pub(super) fn active_when_quiescent(
    state: &TableState,
    readiness: &ReadinessInputs,
) -> ActiveAction {
    match active_after_commit(state, readiness.current_lsn) {
        ActiveAction::Keep => active_after_flush(state, readiness).into(),
        action => action,
    }
}

/// Decides the action after a COMMIT for a table without a running worker.
///
/// `SyncDone` waits for a readiness evaluation; every other syncing state needs
/// a worker.
pub(super) fn idle_after_commit(state: &TableState) -> IdleAction {
    match state {
        TableState::SyncDone { .. } => IdleAction::Keep,
        _ => IdleAction::SpawnWorker,
    }
}

/// Decides the action after a durable flush, or while quiescent, for a table
/// without a running worker.
pub(super) fn idle_after_flush(state: &TableState, readiness: &ReadinessInputs) -> IdleAction {
    match state {
        TableState::SyncDone { lsn, .. } => {
            readiness.ready_proof(*lsn).map_or(IdleAction::Keep, IdleAction::PromoteToReady)
        }
        _ => IdleAction::SpawnWorker,
    }
}

#[cfg(test)]
mod tests {
    use tokio_postgres::types::PgLsn;

    use crate::{
        error::ErrorKind,
        etl_error,
        replication::{
            apply::handoff::{
                ActiveAction, ActiveFlushAction, IdleAction, ReadinessInputs, active_after_commit,
                active_after_flush, active_when_quiescent, idle_after_commit, idle_after_flush,
            },
            state::{TableRetryPolicy, TableState},
        },
    };

    /// LSNs whose pairwise orders cover every comparison the handoff makes,
    /// including the maximum value.
    const LSNS: [u64; 4] = [0, 1, 2, u64::MAX];

    /// Point at which the apply worker evaluates syncing tables.
    #[derive(Debug, Clone, Copy)]
    enum Trigger {
        AfterCommit,
        AfterFlush,
        Quiescent,
    }

    /// Effect of one handoff decision, independent of the action types.
    #[derive(Debug, PartialEq, Eq)]
    enum Effect {
        StartCatchup(PgLsn),
        AwaitCatchup(PgLsn),
        PromoteToReady(PgLsn),
        SpawnWorker,
        Keep,
    }

    impl From<ActiveAction> for Effect {
        fn from(action: ActiveAction) -> Self {
            match action {
                ActiveAction::StartCatchup { catchup_lsn } => Self::StartCatchup(catchup_lsn),
                ActiveAction::AwaitCatchup { catchup_lsn } => Self::AwaitCatchup(catchup_lsn),
                ActiveAction::PromoteToReady(proof) => Self::PromoteToReady(proof.sync_done_lsn()),
                ActiveAction::Keep => Self::Keep,
            }
        }
    }

    impl From<ActiveFlushAction> for Effect {
        fn from(action: ActiveFlushAction) -> Self {
            ActiveAction::from(action).into()
        }
    }

    impl From<IdleAction> for Effect {
        fn from(action: IdleAction) -> Self {
            match action {
                IdleAction::PromoteToReady(proof) => Self::PromoteToReady(proof.sync_done_lsn()),
                IdleAction::SpawnWorker => Self::SpawnWorker,
                IdleAction::Keep => Self::Keep,
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

    /// Runs the decision the apply worker uses for a trigger and presence.
    fn decide(
        trigger: Trigger,
        has_active_worker: bool,
        state: &TableState,
        current_lsn: PgLsn,
        readiness: &ReadinessInputs,
    ) -> Effect {
        match (trigger, has_active_worker) {
            (Trigger::AfterCommit, true) => active_after_commit(state, current_lsn).into(),
            (Trigger::AfterCommit, false) => idle_after_commit(state).into(),
            (Trigger::AfterFlush, true) => active_after_flush(state, readiness).into(),
            (Trigger::Quiescent, true) => active_when_quiescent(state, readiness).into(),
            (Trigger::AfterFlush | Trigger::Quiescent, false) => {
                idle_after_flush(state, readiness).into()
            }
        }
    }

    /// Returns the specified handoff effect.
    ///
    /// One row per trigger and worker presence, transcribed from the
    /// per-trigger handoff functions the decisions replaced. The decisions
    /// must match it on every input.
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
            (Trigger::AfterCommit, true) => match state {
                TableState::SyncWait { lsn } => Effect::StartCatchup((*lsn).max(current_lsn)),
                TableState::Catchup { lsn } => Effect::AwaitCatchup(*lsn),
                _ => Effect::Keep,
            },
            (Trigger::AfterCommit, false) => match state {
                TableState::SyncDone { .. } => Effect::Keep,
                _ => Effect::SpawnWorker,
            },
            (Trigger::AfterFlush, true) => match state {
                TableState::SyncDone { lsn, .. } if ready(*lsn) => Effect::PromoteToReady(*lsn),
                _ => Effect::Keep,
            },
            (Trigger::AfterFlush, false) => match state {
                TableState::SyncDone { lsn, .. } if ready(*lsn) => Effect::PromoteToReady(*lsn),
                TableState::SyncDone { .. } => Effect::Keep,
                _ => Effect::SpawnWorker,
            },
            (Trigger::Quiescent, true) => match state {
                TableState::SyncWait { lsn } => Effect::StartCatchup((*lsn).max(current_lsn)),
                TableState::SyncDone { lsn, .. } if ready(*lsn) => Effect::PromoteToReady(*lsn),
                TableState::Catchup { lsn } => Effect::AwaitCatchup(*lsn),
                _ => Effect::Keep,
            },
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
                                    current_lsn,
                                    persisted_checkpoint_lsn,
                                    has_materialized_decoding_state,
                                );

                                assert_eq!(
                                    decide(
                                        trigger,
                                        has_active_worker,
                                        &state,
                                        current_lsn,
                                        &readiness
                                    ),
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

    /// Generated checks of the readiness laws over the full LSN range.
    #[cfg(feature = "test-utils")]
    mod readiness_properties {
        use proptest::{arbitrary::any, option, prop_assert};
        use tokio_postgres::types::PgLsn;

        use crate::{
            replication::apply::handoff::ReadinessInputs, test_utils::property::run_property,
        };

        #[test]
        fn readiness_never_regresses_as_progress_advances() {
            let strategy = (
                any::<u64>(),
                any::<u64>(),
                option::of(any::<u64>()),
                any::<bool>(),
                any::<u64>(),
                any::<u64>(),
            );

            run_property(
                "readiness is monotonic in progress",
                &strategy,
                |&(sync_done, current, persisted, decoder, current_gain, persisted_gain)| {
                    let before = ReadinessInputs::new(
                        PgLsn::from(current),
                        persisted.map(PgLsn::from),
                        decoder,
                    );
                    let after = ReadinessInputs::new(
                        PgLsn::from(current.saturating_add(current_gain)),
                        persisted
                            .map(|persisted| PgLsn::from(persisted.saturating_add(persisted_gain))),
                        decoder,
                    );
                    let sync_done = PgLsn::from(sync_done);

                    prop_assert!(
                        before.ready_proof(sync_done).is_none()
                            || after.ready_proof(sync_done).is_some()
                    );

                    Ok(())
                },
            );
        }
    }
}
