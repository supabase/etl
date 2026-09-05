use std::time::Duration;

use tokio::time::{sleep, timeout};

use crate::{
    error::{ErrorKind, EtlResult},
    etl_error,
    schema::TableId,
    store::{StateStore, TableStateType},
};

/// Interval between persistent table-state reads while waiting for a condition.
const TABLE_STATE_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Waits until a table has completed its initial synchronization.
///
/// Returns an error when the store read fails or `timeout_duration` elapses.
pub async fn wait_for_table_sync_complete<S>(
    store: &S,
    table_id: TableId,
    timeout_duration: Duration,
) -> EtlResult<()>
where
    S: StateStore,
{
    let wait = async {
        loop {
            let state = store.get_table_state(table_id).await?;
            if state.as_ref().is_some_and(|state| state.as_type().has_completed_table_sync()) {
                return Ok(());
            }

            sleep(TABLE_STATE_POLL_INTERVAL).await;
        }
    };

    timeout(timeout_duration, wait).await.map_err(|_| {
        etl_error!(
            ErrorKind::Unknown,
            "Timed out waiting for table synchronization",
            format!(
                "Table {table_id} did not complete synchronization within {timeout_duration:?}"
            )
        )
    })?
}

/// Waits until a table reaches the expected state type.
///
/// Returns an error when the store read fails or `timeout_duration` elapses.
pub async fn wait_for_table_state_type<S>(
    store: &S,
    table_id: TableId,
    expected_state: TableStateType,
    timeout_duration: Duration,
) -> EtlResult<()>
where
    S: StateStore,
{
    let wait = async {
        loop {
            let state = store.get_table_state(table_id).await?;
            if state.as_ref().is_some_and(|state| state.as_type() == expected_state) {
                return Ok(());
            }

            sleep(TABLE_STATE_POLL_INTERVAL).await;
        }
    };

    timeout(timeout_duration, wait).await.map_err(|_| {
        etl_error!(
            ErrorKind::Unknown,
            "Timed out waiting for table state",
            format!(
                "Table {table_id} did not reach {expected_state:?} within {timeout_duration:?}"
            )
        )
    })?
}
