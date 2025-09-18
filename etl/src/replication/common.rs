use etl_postgres::types::TableId;
use std::collections::HashMap;

use crate::error::EtlResult;
use crate::state::table::TableReplicationPhase;
use crate::store::state::StateStore;

/// Returns the table replication states that are not yet done.
///
/// A table is considered in done state when the apply worker doesn't need to start/restart a table
/// sync worker to make that table progress.
pub async fn get_active_table_replication_states<S>(
    state_store: &S,
) -> EtlResult<HashMap<TableId, TableReplicationPhase>>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    let mut table_replication_states = state_store.get_table_replication_states().await?;
    table_replication_states.retain(|_table_id, state| !state.as_type().is_done());

    Ok(table_replication_states)
}
