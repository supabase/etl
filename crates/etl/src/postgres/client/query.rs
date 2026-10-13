use pg_escape::quote_identifier;
use tokio_postgres::{SimpleQueryMessage, Transaction, error::SqlState, types::PgLsn};

use super::{
    types::{CreateSlotResult, SnapshotAction},
    utils::get_row_value,
};
use crate::{
    bail,
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    schema::TableId,
};

/// Classifies lock failures at copy boundaries while preserving their cause.
///
/// Other query failures retain their existing classification and retry policy.
pub(super) fn classify_table_copy_error(error: impl Into<EtlError>) -> EtlError {
    let error = error.into();
    if error.kind() == ErrorKind::SourceLockTimeout {
        etl_error!(
            ErrorKind::SourceTableCopyLockConflict,
            "Source lock conflict interrupted table copy"
        )
        .with_source(error)
    } else {
        error
    }
}

/// Builds a `CREATE_REPLICATION_SLOT` command for a logical `pgoutput` slot.
fn create_slot_query(slot_name: &str, snapshot_action: SnapshotAction, failover: bool) -> String {
    if failover {
        // PostgreSQL's legacy syntax accepts the snapshot action but has no
        // place for FAILOVER. The parenthesized PostgreSQL 17+ syntax combines
        // both.
        let snapshot_option = match snapshot_action {
            SnapshotAction::Use => "'use'",
            SnapshotAction::NoExport => "'nothing'",
        };
        format!(
            r#"CREATE_REPLICATION_SLOT {} LOGICAL pgoutput (SNAPSHOT {}, FAILOVER)"#,
            quote_identifier(slot_name),
            snapshot_option
        )
    } else {
        // Retain the legacy form for compatibility with PostgreSQL 14 through
        // 16.
        let snapshot_option = match snapshot_action {
            SnapshotAction::Use => "USE_SNAPSHOT",
            SnapshotAction::NoExport => "NOEXPORT_SNAPSHOT",
        };
        format!(
            r#"CREATE_REPLICATION_SLOT {} LOGICAL pgoutput {}"#,
            quote_identifier(slot_name),
            snapshot_option
        )
    }
}

/// Private executor for query helpers shared by replication transactions.
#[derive(Clone, Copy)]
pub(super) struct PgReplicationQueryTarget<'a, 'tx> {
    /// The transaction used to execute queries.
    transaction: &'a Transaction<'tx>,
}

impl<'a, 'tx> PgReplicationQueryTarget<'a, 'tx> {
    /// Wraps an open replication transaction.
    pub(super) fn new(transaction: &'a Transaction<'tx>) -> Self {
        Self { transaction }
    }

    /// Creates a replication slot on this target.
    pub(super) async fn create_slot(
        self,
        slot_name: &str,
        snapshot_action: SnapshotAction,
        failover: bool,
    ) -> EtlResult<CreateSlotResult> {
        // Do not convert the query or the options to lowercase, since the lexer
        // for replication commands (repl_scanner.l) in Postgres code expects
        // the commands in uppercase. This probably should be fixed in upstream,
        // but for now we will keep the commands in uppercase.
        let query = create_slot_query(slot_name, snapshot_action, failover);
        match self.transaction.simple_query(&query).await {
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
                if err.code() == Some(&SqlState::T_R_DEADLOCK_DETECTED) {
                    return Err(etl_error!(
                        ErrorKind::SourceLockTimeout,
                        "Replication slot creation conflicted with source DDL"
                    )
                    .with_source(err));
                }
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

        for msg in self.transaction.simple_query(&query).await? {
            if let SimpleQueryMessage::Row(_) = msg {
                return Ok(true);
            }
        }

        Ok(false)
    }
}
