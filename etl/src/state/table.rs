use std::fmt;
use tokio_postgres::types::PgLsn;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TableReplicationPhase {
    /// Set by the pipeline when it first starts and encounters a table for the first time
    Init,

    /// Set by table-sync worker just before starting initial table copy
    DataSync,

    /// Set by table-sync worker when initial table copy is done
    FinishedCopy,

    /// Set by table-sync worker when waiting for the apply worker to pause
    /// On every transaction boundary the apply worker checks if any table-sync
    /// worker is in the SyncWait state and pauses itself if it finds any.
    /// This phase is stored in memory only and not persisted to the state store
    SyncWait,

    /// Set by the apply worker when it is paused. The table-sync worker waits
    /// for the apply worker to set this state after setting the state to SyncWait.
    /// This phase is stored in memory only and not persisted to the state store
    Catchup {
        /// The lsn to catch up to. This is the location where the apply worker is paused
        lsn: PgLsn,
    },

    /// Set by the table-sync worker when catch-up phase is completed and table-sync
    /// worker has caught up with the apply worker's lsn position
    SyncDone {
        /// The lsn up to which the table-sync worker has caught up
        lsn: PgLsn,
    },

    /// Set by apply worker when it has caught up with the table-sync worker's
    /// catch up lsn position. Tables with this state have successfully run their
    /// initial table copy and catch-up phases and any changes to them will now
    /// be applied by the apply worker only
    Ready,

    /// Set by either the table-sync worker or the apply worker when a table is no
    /// longer being synced because of an error. Tables in this state can only
    /// start syncing again after a manual intervention from the user.
    Skipped,
}

impl TableReplicationPhase {
    pub fn as_type(&self) -> TableReplicationPhaseType {
        self.into()
    }
}

// TODO: we may not need as many phases as we have now.
// Evaluate this once the code is more mature.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TableReplicationPhaseType {
    Init,
    DataSync,
    FinishedCopy,
    SyncWait,
    Catchup,
    SyncDone,
    Ready,
    Skipped,
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
            Self::Skipped => true,
        }
    }

    /// Returns `true` if a table with this phase is done processing, `false` otherwise.
    ///
    /// A table is done processing, when its events are being processed by the apply worker instead
    /// of a table sync worker.
    pub fn is_done(&self) -> bool {
        match self {
            Self::Init => false,
            Self::DataSync => false,
            Self::FinishedCopy => false,
            Self::SyncWait => false,
            Self::Catchup => false,
            Self::SyncDone => false,
            Self::Ready => true,
            Self::Skipped => true,
        }
    }
}

impl<'a> From<&'a TableReplicationPhase> for TableReplicationPhaseType {
    fn from(phase: &'a TableReplicationPhase) -> Self {
        match phase {
            TableReplicationPhase::Init => Self::Init,
            TableReplicationPhase::DataSync => Self::DataSync,
            TableReplicationPhase::FinishedCopy => Self::FinishedCopy,
            TableReplicationPhase::SyncWait => Self::SyncWait,
            TableReplicationPhase::Catchup { .. } => Self::Catchup,
            TableReplicationPhase::SyncDone { .. } => Self::SyncDone,
            TableReplicationPhase::Ready => Self::Ready,
            TableReplicationPhase::Skipped => Self::Skipped,
        }
    }
}

impl fmt::Display for TableReplicationPhaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Init => write!(f, "init"),
            Self::DataSync => write!(f, "data_sync"),
            Self::FinishedCopy => write!(f, "finished_copy"),
            Self::SyncWait => write!(f, "sync_wait"),
            Self::Catchup => write!(f, "catchup"),
            Self::SyncDone => write!(f, "sync_done"),
            Self::Ready => write!(f, "ready"),
            Self::Skipped => write!(f, "skipped"),
        }
    }
}
