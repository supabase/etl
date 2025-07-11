use postgres::schema::TableId;
use std::collections::HashMap;

use crate::state::store::base::StateStore;
use crate::state::table::TableReplicationPhase;
use crate::workers::apply::ApplyWorkerHookError;

/// Returns the table replication states that are either done or in active state.
///
/// A table is considered in done state when the apply worker doesn't need to start/restart a table
/// sync worker to make that table progress.
pub async fn get_table_replication_states<S>(
    state_store: &S,
    done: bool,
) -> Result<HashMap<TableId, TableReplicationPhase>, ApplyWorkerHookError>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    let mut table_replication_states = state_store.get_table_replication_states().await?;
    table_replication_states.retain(|_table_id, state| match done {
        true => state.as_type().is_done(),
        false => !state.as_type().is_done(),
    });

    Ok(table_replication_states)
}
