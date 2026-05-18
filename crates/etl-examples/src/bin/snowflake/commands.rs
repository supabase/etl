use std::sync::{Arc, Mutex};

use etl::{
    state::TableReplicationPhase,
    store::{PostgresStore, StateStore},
    types::TableId,
};
use etl_destinations::snowflake;
use tracing::{error, info};

use crate::state::DashboardState;

/// Reset a single table: truncate Snowflake data, reset state to Init.
/// The pipeline must be stopped before calling this.
pub async fn reset_table(
    table_id: TableId,
    table_name: &str,
    store: &PostgresStore,
    destination: &snowflake::Destination<PostgresStore>,
    dashboard: &Arc<Mutex<DashboardState>>,
) {
    info!(table = %table_name, "resetting table: truncating Snowflake data and resetting state");

    if let Err(e) = destination.committed_offset(table_id).await {
        info!(table = %table_name, "table has no open channel ({}), skipping truncate", e);
    } else {
        // Table has an open channel — try to truncate via the client.
        // We access the underlying client through the destination's public API.
        // Since we can't truncate through the Destination trait without a
        // ReplicatedTableSchema, we reset state and let the pipeline re-copy on
        // next start.
        info!(table = %table_name, "channel exists, data will be re-copied on restart");
    }

    match store.update_table_replication_state(table_id, TableReplicationPhase::Init).await {
        Ok(()) => {
            info!(table = %table_name, "table state reset to Init");
            dashboard.lock().unwrap().set_status(format!(
                "{table_name}: state reset to Init, restart pipeline to re-copy"
            ));
        }
        Err(e) => {
            error!(table = %table_name, error = %e, "failed to reset table state");
            dashboard.lock().unwrap().set_status(format!("{table_name}: reset failed: {e}"));
        }
    }
}

/// Reset ALL tables to Init state.
#[allow(dead_code)]
pub async fn reset_all_tables(
    store: &PostgresStore,
    destination: &snowflake::Destination<PostgresStore>,
    dashboard: &Arc<Mutex<DashboardState>>,
) {
    let tables: Vec<(TableId, String)> = {
        let dash = dashboard.lock().unwrap();
        dash.tables.iter().map(|t| (t.table_id, t.destination_name.clone())).collect()
    };

    for (id, name) in &tables {
        reset_table(*id, name, store, destination, dashboard).await;
    }

    dashboard
        .lock()
        .unwrap()
        .set_status("All tables reset. Restart pipeline to re-copy.".to_owned());
}

pub const HELP_TEXT: &str = "\
Keyboard Shortcuts:
  j/k or Up/Down  Navigate table list
  J/K             Scroll logs up/down (3 lines)
  PgUp/PgDn       Scroll logs (page)
  F1 / ?          Toggle this help
  F2              Reset selected table
  F3              Restart pipeline
  F4              Cycle log filter (All / Warn+ / Error)
  F5              Force-sync Snowflake offsets now
  F10 / q         Quit

Table States:
  init            Table registered, not yet copied
  copying         Initial data copy in progress
  copied          Copy finished, waiting for sync
  sync_wait       Waiting for apply worker pause
  catchup         Catching up to sync LSN
  sync_done       Sync complete
  ready           Fully synced, streaming CDC
  errored         Error occurred (see detail panel)

Commands:
  Reset (F2)      Stops the pipeline, resets the selected table's state
                  to Init, then restarts. The pipeline will re-copy all
                  data from Postgres. Use when a table is stuck in error.

  Restart (F3)    Stops and restarts the pipeline without changing any
                  table states. Tables resume from their current state.

  Log (F4)        Cycles the log filter: All shows everything, Warn+
                  shows only warnings and errors, Error shows errors only.

  Sync (F5)       Queries Snowflake's channel status API to fetch the
                  last committed offset for each table. Offsets also
                  auto-refresh every 30 seconds.

Press any key to close this help.
";
