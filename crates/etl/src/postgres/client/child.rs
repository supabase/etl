use pg_escape::quote_literal;

use super::{raw::PgReplicationClient, transaction::PgChildReplicationTransaction};
use crate::error::EtlResult;

/// A non-replication child connection for snapshot-sharing copy work.
///
/// Child connections are created from a parent
/// [`crate::postgres::client::PgReplicationTransaction`] and import
/// snapshots exported by that transaction. The exporting parent transaction
/// must stay open for at least as long as child transactions depend on its
/// snapshot. The table-copy orchestration upholds this by joining child copy
/// tasks before committing the parent transaction, but this parent-child
/// lifetime relationship is not encoded statically because the child
/// connections are moved into spawned async tasks.
#[derive(Debug)]
pub struct ChildPgReplicationClient {
    /// The actual child connection used for queries.
    client: PgReplicationClient,
}

impl ChildPgReplicationClient {
    /// Wraps a newly-created child connection.
    pub(super) fn new(client: PgReplicationClient) -> Self {
        Self { client }
    }

    /// Begins a read-only repeatable-read transaction pinned to `snapshot_id`.
    ///
    /// The snapshot must have been exported by a still-open parent
    /// [`crate::postgres::client::PgReplicationTransaction`].
    pub async fn begin_transaction(
        &mut self,
        snapshot_id: &str,
    ) -> EtlResult<PgChildReplicationTransaction<'_>> {
        let server_version = self.client.server_version();
        let connection_updates_rx = self.client.connection_updates_rx();
        let transaction = self.client.begin_tx().await?;

        transaction
            .simple_query(&format!("set transaction snapshot {};", quote_literal(snapshot_id)))
            .await?;

        Ok(PgChildReplicationTransaction::new(transaction, server_version, connection_updates_rx))
    }
}
