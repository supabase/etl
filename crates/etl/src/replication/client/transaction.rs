use std::{collections::HashSet, fmt, num::NonZeroI32};

use tokio::sync::watch;
use tokio_postgres::{CopyOutStream, SimpleQueryMessage, Transaction};

use super::{
    child::ChildPgReplicationClient,
    query::PgReplicationQueryTarget,
    raw::PgReplicationClient,
    types::{CtidPartition, PostgresConnectionUpdate},
};
use crate::{
    config::PgConnectionConfig,
    conversions::IdentityMessage,
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{ColumnSchema, TableId, TableSchema},
};

/// A transaction that operates within the context of a replication slot.
///
/// The transaction borrows the client that created the slot. As long as this
/// value is alive, the parent connection cannot be used for other commands.
/// Child connections forked from this transaction rely on snapshots exported by
/// it and must finish their snapshot-dependent work before this transaction is
/// committed.
pub struct PgReplicationTransaction<'a> {
    /// Common transaction state and query helpers.
    core: PgReplicationTransactionCore<'a>,
    /// Settings used to fork child connections that import this transaction's
    /// snapshot.
    pg_connection_config: PgConnectionConfig,
}

impl fmt::Debug for PgReplicationTransaction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgReplicationTransaction").field("core", &self.core).finish_non_exhaustive()
    }
}

/// Common state for an open replication transaction.
///
/// This stays private so parent and child transaction wrappers can expose
/// different capabilities while reusing the same transaction plumbing.
struct PgReplicationTransactionCore<'a> {
    /// The underlying PostgreSQL transaction.
    transaction: Transaction<'a>,
    /// The server version observed on the owning connection.
    server_version: Option<NonZeroI32>,
    /// Updates emitted by the owning connection task.
    connection_updates_rx: watch::Receiver<PostgresConnectionUpdate>,
}

impl fmt::Debug for PgReplicationTransactionCore<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgReplicationTransactionCore")
            .field("server_version", &self.server_version)
            .finish_non_exhaustive()
    }
}

impl<'a> PgReplicationTransactionCore<'a> {
    /// Wraps a transaction and its connection metadata.
    fn new(
        transaction: Transaction<'a>,
        server_version: Option<NonZeroI32>,
        connection_updates_rx: watch::Receiver<PostgresConnectionUpdate>,
    ) -> Self {
        Self { transaction, server_version, connection_updates_rx }
    }

    /// Returns this transaction as a query target.
    fn target(&self) -> PgReplicationQueryTarget<'_, 'a> {
        PgReplicationQueryTarget::Transaction(&self.transaction)
    }

    /// Retrieves the schema information for the supplied table.
    async fn get_table_schema(&self, table_id: TableId) -> EtlResult<TableSchema> {
        self.target().get_table_schema(table_id).await
    }

    /// Retrieves the schema and identity information for the supplied table.
    async fn get_table_schema_with_identity(
        &self,
        table_id: TableId,
    ) -> EtlResult<(TableSchema, IdentityMessage)> {
        self.target().get_table_schema_with_identity(table_id).await
    }

    /// Retrieves the names of columns being replicated for a table in a
    /// publication.
    async fn get_replicated_column_names(
        &self,
        table_id: TableId,
        table_schema: &TableSchema,
        publication_name: &str,
    ) -> EtlResult<HashSet<String>> {
        self.target()
            .get_replicated_column_names(
                self.server_version,
                table_id,
                table_schema,
                publication_name,
            )
            .await
    }

    /// Creates a COPY stream for reading data from the specified table.
    async fn get_table_copy_stream(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
    ) -> EtlResult<CopyOutStream> {
        self.target()
            .get_table_copy_stream(self.server_version, table_id, column_schemas, publication_name)
            .await
    }

    /// Exports the current transaction snapshot.
    async fn export_snapshot(&self) -> EtlResult<String> {
        let results = self.transaction.simple_query("select pg_export_snapshot();").await?;

        for msg in results {
            if let SimpleQueryMessage::Row(row) = msg {
                let snapshot_id = row
                    .try_get(0)?
                    .ok_or_else(|| {
                        etl_error!(
                            ErrorKind::InvalidState,
                            "PostgreSQL pg_export_snapshot returned NULL"
                        )
                    })?
                    .to_owned();
                return Ok(snapshot_id);
            }
        }

        Err(etl_error!(ErrorKind::InvalidState, "PostgreSQL pg_export_snapshot returned no rows"))
    }

    /// Computes balanced ctid partition ranges using page-based estimation.
    async fn plan_ctid_partitions(
        &self,
        table_id: TableId,
        num_partitions: u16,
    ) -> EtlResult<Vec<CtidPartition>> {
        self.target().plan_ctid_partitions(table_id, num_partitions).await
    }

    /// Checks whether the given table is a partitioned parent (`relkind =
    /// 'p'`).
    async fn is_partitioned_table(&self, table_id: TableId) -> EtlResult<bool> {
        self.target().has_partitioned_tables(&[table_id]).await
    }

    /// Returns the OIDs of all leaf partitions for a partitioned table.
    async fn get_leaf_partitions(&self, table_id: TableId) -> EtlResult<Vec<TableId>> {
        self.target().get_leaf_partitions(table_id).await
    }

    /// Creates a COPY stream for a ctid partition range of the specified table.
    async fn get_table_copy_stream_with_ctid_partition(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
        partition: &CtidPartition,
    ) -> EtlResult<CopyOutStream> {
        self.target()
            .get_table_copy_stream_with_ctid_partition(
                self.server_version,
                table_id,
                column_schemas,
                publication_name,
                partition,
            )
            .await
    }

    /// Returns a receiver for background connection task updates.
    fn connection_updates_rx(&self) -> watch::Receiver<PostgresConnectionUpdate> {
        self.connection_updates_rx.clone()
    }

    /// Commits the current transaction.
    async fn commit(self) -> EtlResult<()> {
        self.transaction.commit().await?;

        Ok(())
    }
}

impl<'a> PgReplicationTransaction<'a> {
    /// Wraps a transaction created from a replication client.
    pub(super) fn new(
        transaction: Transaction<'a>,
        pg_connection_config: PgConnectionConfig,
        server_version: Option<NonZeroI32>,
        connection_updates_rx: watch::Receiver<PostgresConnectionUpdate>,
    ) -> Self {
        let core =
            PgReplicationTransactionCore::new(transaction, server_version, connection_updates_rx);

        Self { core, pg_connection_config }
    }

    /// Retrieves the schema information for the supplied table.
    pub async fn get_table_schema(&self, table_id: TableId) -> EtlResult<TableSchema> {
        self.core.get_table_schema(table_id).await
    }

    /// Retrieves the schema and identity information for the supplied table.
    pub(crate) async fn get_table_schema_with_identity(
        &self,
        table_id: TableId,
    ) -> EtlResult<(TableSchema, IdentityMessage)> {
        self.core.get_table_schema_with_identity(table_id).await
    }

    /// Retrieves the names of columns being replicated for a table in a
    /// publication.
    ///
    /// Returns a [`HashSet`] containing the names of columns from the given
    /// [`TableSchema`] that are included in the specified publication for
    /// the given [`TableId`].
    pub async fn get_replicated_column_names(
        &self,
        table_id: TableId,
        table_schema: &TableSchema,
        publication_name: &str,
    ) -> EtlResult<HashSet<String>> {
        self.core.get_replicated_column_names(table_id, table_schema, publication_name).await
    }

    /// Creates a COPY stream for reading data from the specified table.
    ///
    /// The stream will include only the columns specified in `column_schemas`.
    pub async fn get_table_copy_stream(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
    ) -> EtlResult<CopyOutStream> {
        self.core.get_table_copy_stream(table_id, column_schemas, publication_name).await
    }

    /// Exports the current transaction snapshot so child connections can share
    /// it.
    ///
    /// Calls `pg_export_snapshot()` within the slot's `REPEATABLE READ`
    /// transaction.
    pub async fn export_snapshot(&self) -> EtlResult<String> {
        self.core.export_snapshot().await
    }

    /// Computes balanced ctid partition ranges using page-based estimation.
    ///
    /// Returns one [`CtidPartition`] per partition, or an empty vec if the
    /// table has no rows.
    pub async fn plan_ctid_partitions(
        &self,
        table_id: TableId,
        num_partitions: u16,
    ) -> EtlResult<Vec<CtidPartition>> {
        self.core.plan_ctid_partitions(table_id, num_partitions).await
    }

    /// Checks whether the given table is a partitioned parent (`relkind =
    /// 'p'`).
    pub(crate) async fn is_partitioned_table(&self, table_id: TableId) -> EtlResult<bool> {
        self.core.is_partitioned_table(table_id).await
    }

    /// Returns the OIDs of all leaf partitions for a partitioned table.
    ///
    /// Walks `pg_inherits` recursively and returns only leaf nodes (`relkind =
    /// 'r'`). For a non-partitioned table this returns an empty vec.
    pub(crate) async fn get_leaf_partitions(&self, table_id: TableId) -> EtlResult<Vec<TableId>> {
        self.core.get_leaf_partitions(table_id).await
    }

    /// Returns a receiver for background connection task updates.
    pub(crate) fn connection_updates_rx(&self) -> watch::Receiver<PostgresConnectionUpdate> {
        self.core.connection_updates_rx()
    }

    /// Creates a new child connection that can import this transaction's
    /// snapshot.
    ///
    /// The returned child is not statically tied to this transaction's lifetime
    /// because child copy work is moved into spawned async tasks. Callers must
    /// keep this parent transaction open until every child transaction that
    /// imports its exported snapshot has finished.
    pub async fn fork_child(&self) -> EtlResult<ChildPgReplicationClient> {
        PgReplicationClient::connect_child_from_config(self.pg_connection_config.clone()).await
    }

    /// Commits the current transaction.
    pub async fn commit(self) -> EtlResult<()> {
        self.core.commit().await
    }
}

/// A transaction on a child connection pinned to an exported snapshot.
///
/// The transaction borrows the child connection for its full lifetime, ensuring
/// no other commands can be issued on that connection until it is committed or
/// dropped. The exported parent snapshot it imports must remain valid for the
/// duration of the child transaction; this dependency is maintained by callers
/// rather than by this type's lifetime.
pub struct PgChildReplicationTransaction<'a> {
    /// Common transaction state and query helpers.
    core: PgReplicationTransactionCore<'a>,
}

impl fmt::Debug for PgChildReplicationTransaction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgChildReplicationTransaction")
            .field("core", &self.core)
            .finish_non_exhaustive()
    }
}

impl<'a> PgChildReplicationTransaction<'a> {
    /// Creates a new transaction for an already pinned child connection.
    pub(super) fn new(
        transaction: Transaction<'a>,
        server_version: Option<NonZeroI32>,
        connection_updates_rx: watch::Receiver<PostgresConnectionUpdate>,
    ) -> Self {
        let core =
            PgReplicationTransactionCore::new(transaction, server_version, connection_updates_rx);

        Self { core }
    }

    /// Creates a COPY stream for reading all data from the specified table.
    ///
    /// Resolves the table name and row filter internally. Used for copying leaf
    /// partitions of a partitioned table.
    pub(crate) async fn get_table_copy_stream(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
    ) -> EtlResult<CopyOutStream> {
        self.core.get_table_copy_stream(table_id, column_schemas, publication_name).await
    }

    /// Creates a COPY stream for a ctid partition range of the specified table.
    ///
    /// Resolves the table name and row filter internally, then streams rows
    /// whose ctid falls within the given partition bounds.
    pub async fn get_table_copy_stream_with_ctid_partition(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
        partition: &CtidPartition,
    ) -> EtlResult<CopyOutStream> {
        self.core
            .get_table_copy_stream_with_ctid_partition(
                table_id,
                column_schemas,
                publication_name,
                partition,
            )
            .await
    }

    /// Returns a receiver for background connection task updates.
    pub(crate) fn connection_updates_rx(&self) -> watch::Receiver<PostgresConnectionUpdate> {
        self.core.connection_updates_rx()
    }

    /// Commits the current transaction.
    pub async fn commit(self) -> EtlResult<()> {
        self.core.commit().await
    }
}
