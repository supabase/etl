//! Test helpers for observing table-sync worker state transitions.

use std::sync::OnceLock;

use tokio::{sync::broadcast, time::timeout};

use crate::{state::TableStateType, test_utils::notify::DEFAULT_NOTIFY_TIMEOUT, types::TableId};

/// Number of table-sync state transitions retained for late receivers.
const CHANNEL_CAPACITY: usize = 1024;

/// Broadcast channel used by test helpers to observe table-sync transitions.
static TABLE_SYNC_STATE_CHANGES: OnceLock<broadcast::Sender<TableSyncStateChange>> =
    OnceLock::new();

/// A table-sync worker state transition observed by test utilities.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TableSyncStateChange {
    /// Table whose worker changed state.
    pub table_id: TableId,
    /// State type before the transition.
    pub from_state_type: TableStateType,
    /// State type after the transition.
    pub to_state_type: TableStateType,
}

impl TableSyncStateChange {
    /// Creates a new [`TableSyncStateChange`].
    pub(crate) fn new(
        table_id: TableId,
        from_state_type: TableStateType,
        to_state_type: TableStateType,
    ) -> Self {
        Self { table_id, from_state_type, to_state_type }
    }
}

/// Receiver for table-sync worker state transitions.
pub struct TableSyncStateReceiver {
    receiver: broadcast::Receiver<TableSyncStateChange>,
}

impl TableSyncStateReceiver {
    /// Waits until the table reaches the expected state type.
    ///
    /// # Panics
    ///
    /// Panics if no matching state transition is observed before the default
    /// test notification timeout elapses.
    pub async fn wait_for_state_type(
        &mut self,
        table_id: TableId,
        expected_state_type: TableStateType,
    ) -> TableSyncStateChange {
        let wait = async {
            loop {
                match self.receiver.recv().await {
                    Ok(change)
                        if change.table_id == table_id
                            && change.to_state_type == expected_state_type =>
                    {
                        return change;
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => {
                        panic!("table-sync state transition channel closed unexpectedly");
                    }
                }
            }
        };

        timeout(DEFAULT_NOTIFY_TIMEOUT, wait).await.unwrap_or_else(|_| {
            panic!(
                "Timed out after {DEFAULT_NOTIFY_TIMEOUT:?} waiting for table-sync state \
                 {expected_state_type:?} for table {table_id}.",
            )
        })
    }

    /// Waits until the table reaches all expected state types in order.
    ///
    /// # Panics
    ///
    /// Panics if any expected transition is not observed before the default
    /// test notification timeout elapses.
    pub async fn wait_for_state_types(
        &mut self,
        table_id: TableId,
        expected_state_types: &[TableStateType],
    ) -> Vec<TableSyncStateChange> {
        let mut changes = Vec::with_capacity(expected_state_types.len());

        for expected_state_type in expected_state_types {
            changes.push(self.wait_for_state_type(table_id, *expected_state_type).await);
        }

        changes
    }
}

/// Subscribes to future table-sync worker state transitions.
pub fn subscribe_table_sync_state_changes() -> TableSyncStateReceiver {
    TableSyncStateReceiver { receiver: table_sync_state_changes_sender().subscribe() }
}

/// Emits a table-sync worker state transition to test subscribers.
pub(crate) fn emit_table_sync_state_change(change: TableSyncStateChange) {
    let _ = table_sync_state_changes_sender().send(change);
}

/// Returns the global transition sender, initializing it on first use.
fn table_sync_state_changes_sender() -> &'static broadcast::Sender<TableSyncStateChange> {
    TABLE_SYNC_STATE_CHANGES.get_or_init(|| {
        let (sender, _receiver) = broadcast::channel(CHANNEL_CAPACITY);
        sender
    })
}
