use pg_escape::quote_identifier;
use tokio_postgres::{Client, SimpleQueryMessage, Transaction, error::SqlState};

use super::{
    types::{CreateSlotResult, SnapshotAction},
    utils::get_row_value,
};
use crate::{
    bail,
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{PgLsn, TableId},
};

/// Private executor for query helpers shared by clients and open transactions.
#[derive(Clone, Copy)]
pub(super) enum PgReplicationQueryTarget<'a, 'tx> {
    /// A plain PostgreSQL client connection.
    Client(&'a Client),
    /// An open PostgreSQL transaction.
    Transaction(&'a Transaction<'tx>),
}

impl PgReplicationQueryTarget<'_, '_> {
    /// Executes a simple query on the target.
    async fn simple_query(
        self,
        query: &str,
    ) -> Result<Vec<SimpleQueryMessage>, tokio_postgres::Error> {
        match self {
            PgReplicationQueryTarget::Client(client) => client.simple_query(query).await,
            PgReplicationQueryTarget::Transaction(transaction) => {
                transaction.simple_query(query).await
            }
        }
    }

    /// Creates a replication slot on this target.
    pub(super) async fn create_slot(
        self,
        slot_name: &str,
        snapshot_action: SnapshotAction,
    ) -> EtlResult<CreateSlotResult> {
        // Do not convert the query or the options to lowercase, since the lexer for
        // replication commands (repl_scanner.l) in Postgres code expects the commands
        // in uppercase. This probably should be fixed in upstream, but for now we will
        // keep the commands in uppercase.
        let snapshot_option = match snapshot_action {
            SnapshotAction::Use => "USE_SNAPSHOT",
            SnapshotAction::NoExport => "NOEXPORT_SNAPSHOT",
        };
        let query = format!(
            r#"CREATE_REPLICATION_SLOT {} LOGICAL pgoutput {}"#,
            quote_identifier(slot_name),
            snapshot_option
        );
        match self.simple_query(&query).await {
            Ok(results) => {
                for result in results {
                    if let SimpleQueryMessage::Row(row) = result {
                        let consistent_point = get_row_value::<PgLsn>(
                            &row,
                            "consistent_point",
                            "pg_replication_slots",
                        )?;
                        let slot = CreateSlotResult { consistent_point };

                        return Ok(slot);
                    }
                }
            }
            Err(err) => {
                if let Some(code) = err.code()
                    && *code == SqlState::DUPLICATE_OBJECT
                {
                    bail!(
                        ErrorKind::ReplicationSlotAlreadyExists,
                        "Replication slot already exists",
                        format!("Replication slot '{}' already exists in database", slot_name)
                    );
                }

                return Err(err.into());
            }
        }

        Err(etl_error!(ErrorKind::ReplicationSlotNotCreated, "Replication slot creation failed"))
    }

    /// Checks if any of the provided table IDs are partitioned tables.
    pub(super) async fn has_partitioned_tables(self, table_ids: &[TableId]) -> EtlResult<bool> {
        if table_ids.is_empty() {
            return Ok(false);
        }

        let table_oids_list =
            table_ids.iter().map(|id| id.0.to_string()).collect::<Vec<_>>().join(", ");

        let query = format!(
            "select 1 from pg_class where oid in ({table_oids_list}) and relkind = 'p' limit 1;"
        );

        for msg in self.simple_query(&query).await? {
            if let SimpleQueryMessage::Row(_) = msg {
                return Ok(true);
            }
        }

        Ok(false)
    }
}
