use crate::error::{ErrorKind, EtlResult};
use crate::utils::tokio::MakeRustlsConnect;
use crate::{bail, etl_error};
use etl_config::shared::{ETL_REPLICATION_OPTIONS, IntoConnectOptions, PgConnectionConfig};
use etl_postgres::replication::extract_server_version;
use etl_postgres::types::convert_type_oid_to_type;
use etl_postgres::types::{ColumnSchema, TableId, TableName, TableSchema};
use etl_postgres::version::POSTGRES_15;
use etl_postgres::{below_version, requires_version};
use futures::Stream;
use pg_escape::{quote_identifier, quote_literal};
use postgres_replication::LogicalReplicationStream;
use rustls::ClientConfig;
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::fmt;
use std::num::NonZeroI32;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;

use tokio_postgres::error::SqlState;
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{
    Client, Config, Connection, CopyOutStream, NoTls, SimpleQueryMessage, SimpleQueryRow, Socket,
    config::ReplicationMode, types::PgLsn,
};
use tracing::{Instrument, error, info, warn};

/// Maximum time to wait for a replication slot deletion to complete.
///
/// Slot deletion uses `WAIT`, which can block until the slot is no longer in use.
/// This timeout ensures calls are bounded and cannot wait forever.
const DELETE_SLOT_TIMEOUT: Duration = Duration::from_secs(30);

/// Spawns a background task to monitor a Postgres connection until it terminates.
fn spawn_postgres_connection<T>(
    connection: Connection<Socket, T::Stream>,
) -> watch::Receiver<PostgresConnectionUpdate>
where
    T: MakeTlsConnect<Socket>,
    T::Stream: Send + 'static,
{
    let (updates_tx, updates_rx) = watch::channel(PostgresConnectionUpdate::Running);
    let span = tracing::Span::current();
    let task = async move {
        let result = connection.await;

        match result {
            Err(err) => {
                let _ = updates_tx.send(PostgresConnectionUpdate::Errored);
                error!(error = %err, "postgres connection error");
            }
            Ok(()) => {
                let _ = updates_tx.send(PostgresConnectionUpdate::Terminated);
                info!("postgres connection terminated");
            }
        }
    }
    .instrument(span);

    // There is no need to track the connection task via the `JoinHandle` since the `Client`, which
    // returned the connection, will automatically terminate the connection when dropped.
    tokio::spawn(task);

    updates_rx
}

/// Updates emitted by the background task driving the PostgreSQL connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresConnectionUpdate {
    /// The connection task is running.
    Running,
    /// The connection task terminated cleanly.
    Terminated,
    /// The connection task exited due to an error.
    Errored,
}

impl PostgresConnectionUpdate {
    /// Returns `true` when this update indicates that the connection has been closed.
    pub fn signals_connection_closed(self) -> bool {
        matches!(self, Self::Terminated | Self::Errored)
    }
}

impl fmt::Display for PostgresConnectionUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Running => write!(f, "running"),
            Self::Terminated => write!(f, "terminated"),
            Self::Errored => write!(f, "errored"),
        }
    }
}

/// Subscription to PostgreSQL connection lifecycle updates.
///
/// This subscription is intended to provide wake-safe connection state observation for stream
/// adapters that may intentionally pause polling their inner stream (e.g. memory backpressure).
/// It allows those adapters to observe terminal connection states without performing lookahead on
/// the replicated item stream.
#[derive(Debug)]
pub struct PgConnectionSubscription {
    current_rx: watch::Receiver<PostgresConnectionUpdate>,
    updates: WatchStream<PostgresConnectionUpdate>,
}

impl PgConnectionSubscription {
    /// Creates a new connection update subscription from a watch receiver.
    pub fn new(updates_rx: watch::Receiver<PostgresConnectionUpdate>) -> Self {
        let updates = WatchStream::from_changes(updates_rx.clone());

        Self {
            current_rx: updates_rx,
            updates,
        }
    }

    /// Returns the latest known connection update.
    pub fn current_update(&self) -> PostgresConnectionUpdate {
        *self.current_rx.borrow()
    }

    /// Polls for a new connection update.
    ///
    /// Returns:
    /// - `Poll::Ready(Some(update))` when there is an unseen update.
    /// - `Poll::Ready(None)` when the connection update channel is closed.
    /// - `Poll::Pending` when no update is available yet.
    pub fn poll_update(&mut self, cx: &mut Context<'_>) -> Poll<Option<PostgresConnectionUpdate>> {
        match std::pin::Pin::new(&mut self.updates).poll_next(cx) {
            Poll::Ready(Some(update)) => Poll::Ready(Some(update)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Internal snapshot action for `CREATE_REPLICATION_SLOT`.
#[derive(Debug)]
enum SnapshotAction {
    /// `USE_SNAPSHOT` - uses the snapshot in the current transaction.
    Use,
    /// `NOEXPORT_SNAPSHOT` - neither exports nor uses the snapshot.
    NoExport,
}

/// Result returned when creating a new replication slot.
///
/// Contains the consistent point LSN that should be used as the starting point
/// for logical replication.
#[derive(Debug, Clone)]
pub struct CreateSlotResult {
    /// The LSN at which the slot was created, representing a consistent point in the WAL.
    pub consistent_point: PgLsn,
}

/// Result returned when retrieving an existing replication slot.
///
/// Contains the confirmed flush LSN indicating how far replication has progressed.
#[derive(Debug, Clone)]
pub struct GetSlotResult {
    /// The LSN up to which changes have been confirmed as processed by ETL.
    pub confirmed_flush_lsn: PgLsn,
}

/// The current state of a replication slot.
///
/// Represents whether a slot is valid and can be used for replication, or has been
/// invalidated by PostgreSQL (e.g., due to exceeding `max_slot_wal_keep_size`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotState {
    /// The slot is valid and can be used for replication.
    Valid,
    /// The slot has been invalidated and cannot be used for replication.
    ///
    /// This typically occurs when the slot falls too far behind the current WAL position
    /// and PostgreSQL removes the required WAL segments.
    Invalidated,
}

/// Result type for operations that either get an existing slot or create a new one.
///
/// This enum distinguishes between whether a slot was newly created or already existed,
/// providing appropriate result data for each case.
#[derive(Debug, Clone)]
pub enum GetOrCreateSlotResult {
    /// A new slot was created with the given consistent point.
    CreateSlot(CreateSlotResult),
    /// An existing slot was found with the given confirmed flush LSN.
    GetSlot(GetSlotResult),
}

impl GetOrCreateSlotResult {
    /// Returns the lsn that should be used as starting LSN during events replication.
    pub fn get_start_lsn(&self) -> PgLsn {
        match self {
            GetOrCreateSlotResult::CreateSlot(result) => result.consistent_point,
            GetOrCreateSlotResult::GetSlot(result) => result.confirmed_flush_lsn,
        }
    }
}

/// A ctid-based partition range for parallel copy.
#[derive(Debug)]
pub enum CtidPartition {
    /// A range with an open lower bound and an exclusive upper bound.
    ///
    /// Matches rows where `ctid < end_tid`.
    OpenStart { end_tid: String },
    /// A range with an inclusive lower bound and an exclusive upper bound.
    ///
    /// Matches rows where `ctid >= start_tid and ctid < end_tid`.
    Closed { start_tid: String, end_tid: String },
    /// A range with an inclusive lower bound and an open upper bound.
    ///
    /// Matches rows where `ctid >= start_tid`.
    OpenEnd { start_tid: String },
}

/// Result of building publication filter SQL components.
#[derive(Debug)]
struct PublicationFilter {
    /// CTEs to include in the WITH clause (empty string if no publication filtering).
    ctes: String,
    /// Predicate to include in the WHERE clause (empty string if no publication filtering).
    predicate: String,
}

/// A transaction that operates within the context of a replication slot.
///
/// This type ensures that the parent connection remains active for the duration of any
/// transaction spawned by that connection for a given slot.
///
/// The `client` is the client that created the slot and must be active for the duration of
/// the transaction for the snapshot of the slot to be consistent.
#[derive(Debug)]
pub struct PgReplicationTransaction {
    client: PgReplicationClient,
}

impl PgReplicationTransaction {
    /// Creates a new transaction within the context of a replication slot.
    ///
    /// The transaction is started with a repeatable read isolation level and uses the
    /// snapshot associated with the provided slot.
    async fn new(client: PgReplicationClient) -> EtlResult<Self> {
        client.begin_tx().await?;

        Ok(Self { client })
    }

    /// Retrieves the schema information for the supplied table.
    ///
    /// If a publication is specified, only columns included in that publication
    /// will be returned.
    pub async fn get_table_schema(
        &self,
        table_id: TableId,
        publication: Option<&str>,
    ) -> EtlResult<TableSchema> {
        self.client.get_table_schema(table_id, publication).await
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
        self.client
            .get_table_copy_stream(table_id, column_schemas, publication_name)
            .await
    }

    /// Exports the current transaction snapshot so child connections can share it.
    ///
    /// Calls `pg_export_snapshot()` within the slot's `REPEATABLE READ` transaction.
    pub async fn export_snapshot(&self) -> EtlResult<String> {
        self.client.export_snapshot().await
    }

    /// Computes balanced ctid partition ranges using page-based estimation.
    ///
    /// Returns one [`CtidPartition`] per partition, or an empty vec if the table has no rows.
    pub async fn plan_ctid_partitions(
        &self,
        table_id: TableId,
        num_partitions: u16,
    ) -> EtlResult<Vec<CtidPartition>> {
        self.client
            .plan_ctid_partitions(table_id, num_partitions)
            .await
    }

    /// Checks whether the given table is a partitioned parent (`relkind = 'p'`).
    pub async fn is_partitioned_table(&self, table_id: TableId) -> EtlResult<bool> {
        self.client.is_partitioned_table(table_id).await
    }

    /// Returns the OIDs of all leaf partitions for a partitioned table.
    ///
    /// Walks `pg_inherits` recursively and returns only leaf nodes (`relkind = 'r'`).
    /// For a non-partitioned table this returns an empty vec.
    pub async fn get_leaf_partitions(&self, table_id: TableId) -> EtlResult<Vec<TableId>> {
        self.client.get_leaf_partitions(table_id).await
    }

    /// Returns the cloned connection that is used in this transaction.
    pub fn get_cloned_client(&self) -> PgReplicationClient {
        self.client.clone()
    }

    /// Commits the current transaction.
    pub async fn commit(self) -> EtlResult<()> {
        self.client.commit_tx().await
    }

    /// Rolls back the current transaction.
    pub async fn rollback(self) -> EtlResult<()> {
        self.client.rollback_tx().await
    }
}

/// A transaction on a child connection pinned to an exported snapshot.
///
/// Created via [`PgReplicationChildTransaction::new`], which begins a read-only
/// repeatable-read transaction and sets it to the supplied snapshot. The child
/// connection shares the same snapshot as the parent, ensuring consistent reads
/// across parallel operations. Catalog queries performed through this transaction
/// see the same database state as the parent connection.
#[derive(Debug)]
pub struct PgReplicationChildTransaction {
    client: ChildPgReplicationClient,
}

impl PgReplicationChildTransaction {
    /// Creates a new child transaction pinned to the given exported snapshot.
    ///
    /// Begins a read-only repeatable-read transaction and sets it to `snapshot_id`,
    /// ensuring reads are consistent with the parent connection's slot snapshot.
    pub async fn new(client: ChildPgReplicationClient, snapshot_id: &str) -> EtlResult<Self> {
        client.client.begin_tx().await?;
        client.client.set_tx_snapshot(snapshot_id).await?;

        Ok(Self { client })
    }

    /// Creates a COPY stream for reading all data from the specified table.
    ///
    /// Resolves the table name and row filter internally. Used for copying leaf
    /// partitions of a partitioned table.
    pub async fn get_table_copy_stream(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
    ) -> EtlResult<CopyOutStream> {
        self.client
            .client
            .get_table_copy_stream(table_id, column_schemas, publication_name)
            .await
    }

    /// Creates a COPY stream for a ctid partition range of the specified table.
    ///
    /// Resolves the table name and row filter internally, then streams rows whose ctid
    /// falls within the given partition bounds.
    pub async fn get_table_copy_stream_with_ctid_partition(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
        partition: &CtidPartition,
    ) -> EtlResult<CopyOutStream> {
        self.client
            .client
            .get_table_copy_stream_with_ctid_partition(
                table_id,
                column_schemas,
                publication_name,
                partition,
            )
            .await
    }

    /// Returns the cloned connection that is used in this child transaction.
    pub fn get_cloned_client(&self) -> PgReplicationClient {
        self.client.client.clone()
    }

    /// Commits the current transaction.
    pub async fn commit(self) -> EtlResult<()> {
        self.client.client.commit_tx().await
    }

    /// Rolls back the current transaction.
    pub async fn rollback(self) -> EtlResult<()> {
        self.client.client.rollback_tx().await
    }
}

/// A non-replication child connection that keeps the parent [`PgReplicationClient`] alive.
///
/// Holding a clone of the parent ensures the main replication connection cannot be dropped
/// while any child exists, providing a compile-time lifetime guarantee via the inner [`Arc`].
#[derive(Debug)]
pub struct ChildPgReplicationClient {
    /// Clone of the parent kept solely to prevent the main connection from being dropped.
    _parent: PgReplicationClient,
    /// The actual child connection used for queries.
    client: PgReplicationClient,
}

/// A client for interacting with Postgres's logical replication features.
///
/// This client provides methods for creating replication slots, managing transactions,
/// and streaming changes from the database.
#[derive(Debug, Clone)]
pub struct PgReplicationClient {
    client: Arc<Client>,
    pg_connection_config: Arc<PgConnectionConfig>,
    server_version: Option<NonZeroI32>,
    connection_updates_rx: watch::Receiver<PostgresConnectionUpdate>,
}

impl PgReplicationClient {
    /// Establishes a connection to Postgres. The connection uses TLS if configured in the
    /// supplied [`PgConnectionConfig`].
    ///
    /// The connection is configured for logical replication mode
    pub async fn connect(pg_connection_config: PgConnectionConfig) -> EtlResult<Self> {
        match pg_connection_config.tls.enabled {
            true => PgReplicationClient::connect_tls(pg_connection_config).await,
            false => PgReplicationClient::connect_no_tls(pg_connection_config).await,
        }
    }

    /// Establishes a connection to Postgres without TLS encryption.
    ///
    /// The connection is configured for logical replication mode.
    async fn connect_no_tls(pg_connection_config: PgConnectionConfig) -> EtlResult<Self> {
        let mut config: Config = pg_connection_config
            .clone()
            .with_db(Some(&ETL_REPLICATION_OPTIONS));
        config.replication_mode(ReplicationMode::Logical);

        let (client, connection) = config.connect(NoTls).await?;

        let server_version = connection
            .parameter("server_version")
            .and_then(extract_server_version);

        let connection_updates_rx = spawn_postgres_connection::<NoTls>(connection);

        info!("connected to postgres without tls");

        Ok(PgReplicationClient {
            client: Arc::new(client),
            pg_connection_config: Arc::new(pg_connection_config),
            server_version,
            connection_updates_rx,
        })
    }

    /// Establishes a TLS-encrypted connection to Postgres.
    ///
    /// The connection is configured for logical replication mode
    async fn connect_tls(pg_connection_config: PgConnectionConfig) -> EtlResult<Self> {
        let mut config: Config = pg_connection_config
            .clone()
            .with_db(Some(&ETL_REPLICATION_OPTIONS));
        config.replication_mode(ReplicationMode::Logical);

        let mut root_store = rustls::RootCertStore::empty();
        if pg_connection_config.tls.enabled {
            for cert in CertificateDer::pem_slice_iter(
                pg_connection_config.tls.trusted_root_certs.as_bytes(),
            ) {
                let cert = cert?;
                root_store.add(cert)?;
            }
        };

        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let (client, connection) = config.connect(MakeRustlsConnect::new(tls_config)).await?;

        let server_version = connection
            .parameter("server_version")
            .and_then(extract_server_version);

        let connection_updates_rx = spawn_postgres_connection::<MakeRustlsConnect>(connection);

        info!("connected to postgres with tls");

        Ok(PgReplicationClient {
            client: Arc::new(client),
            pg_connection_config: Arc::new(pg_connection_config),
            server_version,
            connection_updates_rx,
        })
    }

    /// Creates a non-replication, non-TLS child connection.
    async fn connect_child_no_tls(&self) -> EtlResult<ChildPgReplicationClient> {
        let config: Config = self
            .pg_connection_config
            .as_ref()
            .clone()
            .with_db(Some(&ETL_REPLICATION_OPTIONS));

        let (client, connection) = config.connect(NoTls).await?;
        let connection_updates_rx = spawn_postgres_connection::<NoTls>(connection);

        let client = PgReplicationClient {
            client: Arc::new(client),
            pg_connection_config: self.pg_connection_config.clone(),
            server_version: self.server_version,
            connection_updates_rx,
        };

        Ok(ChildPgReplicationClient {
            _parent: self.clone(),
            client,
        })
    }

    /// Creates a non-replication, TLS-encrypted child connection.
    async fn connect_child_tls(&self) -> EtlResult<ChildPgReplicationClient> {
        let config: Config = self
            .pg_connection_config
            .as_ref()
            .clone()
            .with_db(Some(&ETL_REPLICATION_OPTIONS));

        let mut root_store = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(
            self.pg_connection_config.tls.trusted_root_certs.as_bytes(),
        ) {
            let cert = cert?;
            root_store.add(cert)?;
        }

        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let (client, connection) = config.connect(MakeRustlsConnect::new(tls_config)).await?;
        let connection_updates_rx = spawn_postgres_connection::<MakeRustlsConnect>(connection);

        let client = PgReplicationClient {
            client: Arc::new(client),
            pg_connection_config: self.pg_connection_config.clone(),
            server_version: self.server_version,
            connection_updates_rx,
        };

        Ok(ChildPgReplicationClient {
            _parent: self.clone(),
            client,
        })
    }

    /// Creates a non-replication child connection that inherits this client's connection settings.
    ///
    /// The child does not set `ReplicationMode::Logical`, so it does not consume a
    /// `max_wal_senders` slot. It holds a clone of the parent to ensure the main connection
    /// stays alive while any child exists.
    pub async fn fork_child(&self) -> EtlResult<ChildPgReplicationClient> {
        match self.pg_connection_config.tls.enabled {
            true => self.connect_child_tls().await,
            false => self.connect_child_no_tls().await,
        }
    }

    /// Checks if the underlying connection is closed.
    pub fn is_closed(&self) -> bool {
        self.client.is_closed()
    }

    /// Returns a receiver for background connection task updates.
    pub fn connection_subscription(&self) -> PgConnectionSubscription {
        PgConnectionSubscription::new(self.connection_updates_rx.clone())
    }

    /// Executes a simple query on the underlying connection and returns all result messages.
    pub async fn simple_query(&self, query: &str) -> EtlResult<Vec<SimpleQueryMessage>> {
        Ok(self.client.simple_query(query).await?)
    }

    /// Creates a new logical replication slot with the specified name and a transaction pinned
    /// to the slot's snapshot.
    ///
    /// A `REPEATABLE READ` transaction is begun first, then the slot is created with
    /// `USE_SNAPSHOT` which pins the transaction to the slot's consistent snapshot. The
    /// transaction must be kept open for the duration of any operations that depend on
    /// this snapshot (e.g. schema fetches, table copies, or `pg_export_snapshot()` calls
    /// for child connections).
    pub async fn create_slot_with_transaction(
        &self,
        slot_name: &str,
    ) -> EtlResult<(PgReplicationTransaction, CreateSlotResult)> {
        // TODO: check if we want to consume the client and return it on commit to avoid any other
        //  operations on a connection that has started a transaction.

        // USE_SNAPSHOT requires being inside a transaction.
        let transaction = PgReplicationTransaction::new(self.clone()).await?;
        let slot = self
            .create_slot_internal(slot_name, SnapshotAction::Use)
            .await?;

        Ok((transaction, slot))
    }

    /// Creates a new logical replication slot with the specified name and no snapshot.
    pub async fn create_slot(&self, slot_name: &str) -> EtlResult<CreateSlotResult> {
        self.create_slot_internal(slot_name, SnapshotAction::NoExport)
            .await
    }

    /// Gets the state of a replication slot by name.
    ///
    /// Queries the `pg_replication_slots` system catalog to determine if the slot exists
    /// and whether it's valid or invalidated. A slot is considered invalidated when its
    /// `wal_status` is 'lost', indicating that required WAL segments have been removed.
    ///
    /// Returns an error if the slot doesn't exist.
    pub async fn get_slot_state(&self, slot_name: &str) -> EtlResult<SlotState> {
        let query = format!(
            r#"select wal_status from pg_replication_slots where slot_name = {};"#,
            quote_literal(slot_name)
        );

        let results = self.client.simple_query(&query).await?;
        for result in results {
            if let SimpleQueryMessage::Row(row) = result {
                // wal_status can be: 'reserved', 'extended', 'unreserved', or 'lost'
                // A slot is invalidated when wal_status is 'lost'
                let wal_status: Option<String> = row.try_get("wal_status")?.map(String::from);

                return match wal_status.as_deref() {
                    Some("lost") => Ok(SlotState::Invalidated),
                    Some(_) => Ok(SlotState::Valid),
                    // If wal_status is NULL, assume the slot is valid
                    // (this can happen on very old PostgreSQL versions)
                    None => Ok(SlotState::Valid),
                };
            }
        }

        bail!(
            ErrorKind::ReplicationSlotNotFound,
            "Replication slot not found",
            format!("Replication slot '{}' not found in database", slot_name)
        );
    }

    /// Gets the slot by `slot_name`.
    ///
    /// Returns an error in case of failure or missing slot.
    pub async fn get_slot(&self, slot_name: &str) -> EtlResult<GetSlotResult> {
        let query = format!(
            r#"select confirmed_flush_lsn from pg_replication_slots where slot_name = {};"#,
            quote_literal(slot_name)
        );

        let results = self.client.simple_query(&query).await?;
        for result in results {
            if let SimpleQueryMessage::Row(row) = result {
                let confirmed_flush_lsn = Self::get_row_value::<PgLsn>(
                    &row,
                    "confirmed_flush_lsn",
                    "pg_replication_slots",
                )
                .await?;
                let slot = GetSlotResult {
                    confirmed_flush_lsn,
                };

                return Ok(slot);
            }
        }

        bail!(
            ErrorKind::ReplicationSlotNotFound,
            "Replication slot not found",
            format!("Replication slot '{}' not found in database", slot_name)
        );
    }

    /// Gets an existing replication slot or creates a new one if it doesn't exist.
    ///
    /// This method first attempts to get the slot by name. If the slot doesn't exist,
    /// it creates a new one.
    ///
    /// Returns a tuple containing:
    /// - A boolean indicating whether the slot was created (true) or already existed (false)
    /// - The slot result containing either the confirmed_flush_lsn (for existing slots)
    ///   or the consistent_point (for newly created slots)
    pub async fn get_or_create_slot(&self, slot_name: &str) -> EtlResult<GetOrCreateSlotResult> {
        match self.get_slot(slot_name).await {
            Ok(slot) => {
                info!(slot_name, "using existing replication slot");

                Ok(GetOrCreateSlotResult::GetSlot(slot))
            }
            Err(err) if err.kind() == ErrorKind::ReplicationSlotNotFound => {
                info!(slot_name, "creating new replication slot");

                let create_result = self.create_slot(slot_name).await?;

                Ok(GetOrCreateSlotResult::CreateSlot(create_result))
            }
            Err(e) => Err(e),
        }
    }

    /// Deletes a replication slot with the specified name.
    ///
    /// Returns an error if the slot doesn't exist or if there are any issues with the deletion.
    pub async fn delete_slot(&self, slot_name: &str) -> EtlResult<()> {
        self.delete_slot_internal(slot_name, true).await
    }

    /// Deletes a replication slot with the specified name if it exists.
    ///
    /// This method returns [`Ok(())`] when the slot is missing and propagates any other
    /// error from [`PgReplicationClient::delete_slot`].
    pub async fn delete_slot_if_exists(&self, slot_name: &str) -> EtlResult<()> {
        self.delete_slot_internal(slot_name, false).await
    }

    /// Deletes a replication slot, optionally failing when the slot does not exist.
    async fn delete_slot_internal(&self, slot_name: &str, fail_if_missing: bool) -> EtlResult<()> {
        info!(slot_name, "deleting replication slot");

        // Do not convert the query or the options to lowercase, see comment in `create_slot_internal`.
        let query = format!(
            r#"DROP_REPLICATION_SLOT {} WAIT;"#,
            quote_identifier(slot_name)
        );

        let delete_result =
            match tokio::time::timeout(DELETE_SLOT_TIMEOUT, self.client.simple_query(&query)).await
            {
                Ok(result) => result,
                Err(err) => {
                    error!(
                        slot_name,
                        timeout_secs = DELETE_SLOT_TIMEOUT.as_secs(),
                        "timed out while deleting replication slot"
                    );

                    bail!(
                        ErrorKind::ReplicationSlotDeletionTimeout,
                        "Replication slot deletion timed out",
                        format!(
                            "Timed out after {:?} while deleting replication slot '{}'",
                            DELETE_SLOT_TIMEOUT, slot_name
                        ),
                        source: err
                    );
                }
            };

        match delete_result {
            Ok(_) => {
                info!(slot_name, "deleted replication slot");

                Ok(())
            }
            Err(err) => {
                if let Some(code) = err.code()
                    && *code == SqlState::UNDEFINED_OBJECT
                {
                    if fail_if_missing {
                        warn!(
                            slot_name,
                            "attempted to delete non-existent replication slot"
                        );

                        bail!(
                            ErrorKind::ReplicationSlotNotFound,
                            "Replication slot not found",
                            format!(
                                "Replication slot '{}' not found in database while attempting its deletion",
                                slot_name
                            )
                        );
                    }

                    info!(slot_name, "replication slot not found, skipping deletion");

                    return Ok(());
                }

                error!(slot_name, error = %err, "failed to delete replication slot");

                Err(err.into())
            }
        }
    }

    /// Checks if a publication with the given name exists.
    pub async fn publication_exists(&self, publication: &str) -> EtlResult<bool> {
        let publication_exists_query = format!(
            "select 1 as exists from pg_publication where pubname = {};",
            quote_literal(publication)
        );
        for msg in self.client.simple_query(&publication_exists_query).await? {
            if let SimpleQueryMessage::Row(_) = msg {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Retrieves the `publish_via_partition_root` setting for a publication.
    ///
    /// Returns `true` if the publication is configured to send replication messages using
    /// the parent table OID, or `false` if it sends them using child partition OIDs.
    pub async fn get_publish_via_partition_root(&self, publication: &str) -> EtlResult<bool> {
        let query = format!(
            "select pubviaroot from pg_publication where pubname = {};",
            quote_literal(publication)
        );

        for msg in self.client.simple_query(&query).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                let pubviaroot =
                    Self::get_row_value::<String>(&row, "pubviaroot", "pg_publication").await?;
                return Ok(pubviaroot == "t");
            }
        }

        bail!(
            ErrorKind::ConfigError,
            "Publication not found",
            format!("Publication '{}' not found in database", publication)
        );
    }

    /// Checks whether a single table is a partitioned parent (`relkind = 'p'`).
    async fn is_partitioned_table(&self, table_id: TableId) -> EtlResult<bool> {
        self.has_partitioned_tables(&[table_id]).await
    }

    /// Returns the OIDs of all leaf partitions for a partitioned table.
    ///
    /// Uses `pg_partition_tree()` (available since PostgreSQL 12) to efficiently walk the
    /// partition hierarchy and return only leaf nodes. For a non-partitioned table this
    /// returns an empty vec.
    async fn get_leaf_partitions(&self, table_id: TableId) -> EtlResult<Vec<TableId>> {
        let query = format!(
            "select relid::oid as oid from pg_partition_tree({table_id}::regclass) \
             where isleaf and relid != {table_id}::regclass \
             order by relid::oid;"
        );

        let mut leaves = Vec::new();
        for msg in self.client.simple_query(&query).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                let oid = Self::get_row_value::<TableId>(&row, "oid", "pg_class").await?;
                leaves.push(oid);
            }
        }

        Ok(leaves)
    }

    /// Checks if any of the provided table IDs are partitioned tables.
    ///
    /// A partitioned table is one where `relkind = 'p'` in `pg_class`.
    /// Returns `true` if at least one table is partitioned, `false` otherwise.
    pub async fn has_partitioned_tables(&self, table_ids: &[TableId]) -> EtlResult<bool> {
        if table_ids.is_empty() {
            return Ok(false);
        }

        let table_oids_list = table_ids
            .iter()
            .map(|id| id.0.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!(
            "select 1 from pg_class where oid in ({table_oids_list}) and relkind = 'p' limit 1;"
        );

        for msg in self.client.simple_query(&query).await? {
            if let SimpleQueryMessage::Row(_) = msg {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Retrieves the names of all tables included in a publication.
    pub async fn get_publication_table_names(
        &self,
        publication_name: &str,
    ) -> EtlResult<Vec<TableName>> {
        let publication_query = format!(
            "select schemaname, tablename from pg_publication_tables where pubname = {};",
            quote_literal(publication_name)
        );

        let mut table_names = vec![];
        for msg in self.client.simple_query(&publication_query).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                let schema =
                    Self::get_row_value::<String>(&row, "schemaname", "pg_publication_tables")
                        .await?;
                let name =
                    Self::get_row_value::<String>(&row, "tablename", "pg_publication_tables")
                        .await?;

                table_names.push(TableName { schema, name })
            }
        }

        Ok(table_names)
    }

    /// Retrieves the OIDs of all tables included in a publication.
    ///
    /// For partitioned tables with `publish_via_partition_root=true`, this returns only the parent
    /// table OID. The query uses a recursive CTE to walk up the partition inheritance hierarchy
    /// and identify root tables that have no parent themselves.
    pub async fn get_publication_table_ids(
        &self,
        publication_name: &str,
    ) -> EtlResult<Vec<TableId>> {
        let query = format!(
            r#"
            with recursive pub_tables as (
                -- Get all tables from publication (pg_publication_tables includes explicit tables,
                -- ALL TABLES publications, and FOR TABLES IN SCHEMA publications)
                select c.oid
                from pg_publication_tables pt
                join pg_class c on c.relname = pt.tablename
                join pg_namespace n on n.oid = c.relnamespace and n.nspname = pt.schemaname
                where pt.pubname = {pub}
            ),
            hierarchy(relid) as (
                -- Start with published tables
                select oid from pub_tables

                union

                -- Recursively find parent tables in inheritance hierarchy
                select i.inhparent
                from pg_inherits i
                join hierarchy h on h.relid = i.inhrelid
            )
            -- Return only root tables (those without a parent)
            select distinct relid as oid
            from hierarchy
            where not exists (
                select 1 from pg_inherits i where i.inhrelid = hierarchy.relid
            );
            "#,
            pub = quote_literal(publication_name)
        );

        let mut roots = vec![];
        for msg in self.client.simple_query(&query).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                let table_id = Self::get_row_value::<TableId>(&row, "oid", "pg_class").await?;
                roots.push(table_id);
            }
        }

        Ok(roots)
    }

    /// Starts a logical replication stream from the specified publication and slot.
    ///
    /// The stream will begin reading changes from the provided `start_lsn`.
    pub async fn start_logical_replication(
        &self,
        publication_name: &str,
        slot_name: &str,
        start_lsn: PgLsn,
    ) -> EtlResult<LogicalReplicationStream> {
        info!(publication_name, slot_name, %start_lsn, "starting logical replication");

        // Do not convert the query or the options to lowercase, see comment in `create_slot_internal`.
        let options = format!(
            r#"("proto_version" '1', "publication_names" {})"#,
            quote_literal(quote_identifier(publication_name).as_ref()),
        );

        let query = format!(
            r#"START_REPLICATION SLOT {} LOGICAL {} {}"#,
            quote_identifier(slot_name),
            start_lsn,
            options
        );

        let copy_stream = self.client.copy_both_simple::<bytes::Bytes>(&query).await?;
        let stream = LogicalReplicationStream::new(copy_stream);

        Ok(stream)
    }

    /// Begins a new transaction with repeatable read isolation level.
    ///
    /// The transaction doesn't make any assumptions about the snapshot in use, since this is a
    /// concern of the statements issued within the transaction.
    async fn begin_tx(&self) -> EtlResult<()> {
        self.client
            .simple_query("begin read only isolation level repeatable read;")
            .await?;

        Ok(())
    }

    /// Sets the snapshot id on the running transaction.
    async fn set_tx_snapshot(&self, snapshot_id: &str) -> EtlResult<()> {
        self.client
            .simple_query(&format!(
                "set transaction snapshot {};",
                quote_literal(snapshot_id)
            ))
            .await?;

        Ok(())
    }

    /// Commits the current transaction.
    async fn commit_tx(&self) -> EtlResult<()> {
        self.client.simple_query("commit;").await?;

        Ok(())
    }

    /// Rolls back the current transaction.
    async fn rollback_tx(&self) -> EtlResult<()> {
        self.client.simple_query("rollback;").await?;

        Ok(())
    }

    /// Internal helper method to create a replication slot.
    ///
    /// The `snapshot_action` controls how the slot's snapshot is handled during creation.
    async fn create_slot_internal(
        &self,
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
        match self.client.simple_query(&query).await {
            Ok(results) => {
                for result in results {
                    if let SimpleQueryMessage::Row(row) = result {
                        let consistent_point = Self::get_row_value::<PgLsn>(
                            &row,
                            "consistent_point",
                            "pg_replication_slots",
                        )
                        .await?;
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
                        format!(
                            "Replication slot '{}' already exists in database",
                            slot_name
                        )
                    );
                }

                return Err(err.into());
            }
        }

        Err(etl_error!(
            ErrorKind::ReplicationSlotNotCreated,
            "Replication slot creation failed"
        ))
    }

    /// Retrieves the schema for a single table.
    ///
    /// If a publication is specified, only columns included in that publication
    /// will be returned.
    async fn get_table_schema(
        &self,
        table_id: TableId,
        publication: Option<&str>,
    ) -> EtlResult<TableSchema> {
        let table_name = self.get_table_name(table_id).await?;
        let column_schemas = self.get_column_schemas(table_id, publication).await?;

        Ok(TableSchema {
            name: table_name,
            id: table_id,
            column_schemas,
        })
    }

    /// Loads the table name and schema information for a given table OID.
    ///
    /// Returns a `TableName` containing both the schema and table name.
    async fn get_table_name(&self, table_id: TableId) -> EtlResult<TableName> {
        let table_info_query = format!(
            "select n.nspname as schema_name, c.relname as table_name
            from pg_class c
            join pg_namespace n on c.relnamespace = n.oid
            where c.oid = {table_id}",
        );

        for message in self.client.simple_query(&table_info_query).await? {
            if let SimpleQueryMessage::Row(row) = message {
                let schema_name =
                    Self::get_row_value::<String>(&row, "schema_name", "pg_namespace").await?;
                let table_name =
                    Self::get_row_value::<String>(&row, "table_name", "pg_class").await?;

                return Ok(TableName {
                    schema: schema_name,
                    name: table_name,
                });
            }
        }

        bail!(
            ErrorKind::SourceSchemaError,
            "Table not found in source database",
            format!("Table with ID {} not found in database", table_id)
        );
    }

    /// Builds SQL fragments for filtering columns based on publication settings.
    ///
    /// Returns CTEs and predicates that filter columns according to:
    /// - Postgres 15+: Column-level filtering using `prattrs`
    /// - Postgres 14 and earlier: Table-level filtering only
    /// - No publication: No filtering (empty strings)
    fn build_publication_filter_sql(
        &self,
        table_id: TableId,
        publication_name: Option<&str>,
    ) -> PublicationFilter {
        let Some(publication_name) = publication_name else {
            return PublicationFilter {
                ctes: String::new(),
                predicate: String::new(),
            };
        };

        // Postgres 15+ supports column-level filtering via prattrs
        if requires_version!(self.server_version, POSTGRES_15) {
            return PublicationFilter {
                ctes: format!(
                    "pub_info as (
                        select p.oid as puboid, p.puballtables, r.prattrs
                        from pg_publication p
                        left join pg_publication_rel r on r.prpubid = p.oid and r.prrelid = {table_id}
                        where p.pubname = {publication}
                    ),
                    pub_attrs as (
                        select unnest(prattrs) as attnum
                        from pub_info
                        where prattrs is not null
                    ),
                    pub_schema as (
                        select 1 as exists_in_schema_pub
                        from pub_info
                        join pg_publication_namespace pn on pn.pnpubid = pub_info.puboid
                        join pg_class c on c.relnamespace = pn.pnnspid
                        where c.oid = {table_id}
                    ),",
                    publication = quote_literal(publication_name),
                ),
                predicate: "and (
                        (select puballtables from pub_info) = true
                        or (select count(*) from pub_schema) > 0
                        or (
                            case (select count(*) from pub_attrs)
                                when 0 then true
                                else (a.attnum in (select attnum from pub_attrs))
                            end
                        )
                    )"
                .to_string(),
            };
        }

        // Postgres 14 and earlier: table-level filtering only
        PublicationFilter {
            ctes: format!(
                "pub_info as (
                    select p.puballtables
                    from pg_publication p
                    where p.pubname = {publication}
                ),
                pub_table as (
                    select 1 as exists_in_pub
                    from pg_publication_rel r
                    join pg_publication p on r.prpubid = p.oid
                    where p.pubname = {publication}
                        and r.prrelid = {table_id}
                ),",
                publication = quote_literal(publication_name),
            ),
            predicate: "and ((select puballtables from pub_info) = true or (select count(*) from pub_table) > 0)".to_string(),
        }
    }

    /// Retrieves schema information for all columns in a table.
    ///
    /// If a publication is specified, only columns included in that publication
    /// will be returned. Generated columns are always excluded since they are not
    /// supported in PostgreSQL logical replication.
    async fn get_column_schemas(
        &self,
        table_id: TableId,
        publication: Option<&str>,
    ) -> EtlResult<Vec<ColumnSchema>> {
        // Build publication filter CTEs and predicates based on Postgres version.
        let publication_filter = self.build_publication_filter_sql(table_id, publication);

        let column_info_query = format!(
            r#"
            with {publication_ctes}
            -- Find the direct parent table (for child partitions)
            direct_parent as (
                select i.inhparent as parent_oid
                from pg_inherits i
                where i.inhrelid = {table_id}
                limit 1
            ),
            -- Extract primary key column names from the parent table
            parent_pk_cols as (
                select array_agg(a.attname order by x.n) as pk_column_names
                from pg_constraint con
                join unnest(con.conkey) with ordinality as x(attnum, n) on true
                join pg_attribute a on a.attrelid = con.conrelid and a.attnum = x.attnum
                join direct_parent dp on con.conrelid = dp.parent_oid
                where con.contype = 'p'
                group by con.conname
            )
            select
                a.attname,
                a.atttypid,
                a.atttypmod,
                a.attnotnull,
                case
                    -- Check if column has a direct primary key index
                    when coalesce(i.indisprimary, false) = true then true
                    -- Check if column name matches parent's primary key (for partitions)
                    when exists (
                        select 1
                        from parent_pk_cols pk
                        where a.attname = any(pk.pk_column_names)
                    ) then true
                    else false
                end as primary
            from pg_attribute a
            left join pg_index i
                on a.attrelid = i.indrelid
                and a.attnum = any(i.indkey)
                and i.indisprimary = true
            where a.attnum > 0::int2
                and not a.attisdropped
                and a.attgenerated = ''
                and a.attrelid = {table_id}
                {publication_predicate}
            order by a.attnum
            "#,
            publication_ctes = publication_filter.ctes,
            publication_predicate = publication_filter.predicate,
        );

        // Check for generated columns so we can warn if there are any.
        let generated_columns_check_query = format!(
            r#"select exists (
                select 1
                from pg_attribute
                where attrelid = {table_id}
                    and attnum > 0
                    and not attisdropped
                    and attgenerated != ''
            ) as has_generated;"#
        );

        for message in self
            .client
            .simple_query(&generated_columns_check_query)
            .await?
        {
            if let SimpleQueryMessage::Row(row) = message {
                let has_generated_columns =
                    Self::get_row_value::<String>(&row, "has_generated", "pg_attribute").await?
                        == "t";
                if has_generated_columns {
                    warn!(
                        "Table {} contains generated columns that will NOT be replicated. \
                         Generated columns are not supported in PostgreSQL logical replication and will \
                         be excluded from the ETL schema. These columns will NOT appear in the destination.",
                        table_id
                    );
                }
                // Explicity break for clarity; this query returns a single SimpleQueryMessage::Row.
                break;
            }
        }

        let mut column_schemas = vec![];
        for message in self.client.simple_query(&column_info_query).await? {
            if let SimpleQueryMessage::Row(row) = message {
                let name = Self::get_row_value::<String>(&row, "attname", "pg_attribute").await?;
                let type_oid = Self::get_row_value::<u32>(&row, "atttypid", "pg_attribute").await?;
                let modifier =
                    Self::get_row_value::<i32>(&row, "atttypmod", "pg_attribute").await?;
                let nullable =
                    Self::get_row_value::<String>(&row, "attnotnull", "pg_attribute").await? == "f";
                let primary =
                    Self::get_row_value::<String>(&row, "primary", "pg_index").await? == "t";

                let typ = convert_type_oid_to_type(type_oid);

                column_schemas.push(ColumnSchema {
                    name,
                    typ,
                    modifier,
                    nullable,
                    primary,
                })
            }
        }

        Ok(column_schemas)
    }

    /// Retrieves the publication row filter for a table.
    /// If no publication is specified, we will always return None
    pub async fn get_row_filter(
        &self,
        table_id: TableId,
        publication_name: Option<&str>,
    ) -> EtlResult<Option<String>> {
        // Row filters on publications were added in Postgres 15. For any earlier versions we know that there is no row filter
        if below_version!(self.server_version, POSTGRES_15) {
            return Ok(None);
        }
        // If we don't have a publication the row filter is implicitly non-existent
        let publication = match publication_name {
            Some(publication) => publication,
            _ => return Ok(None),
        };

        // This uses the same query as the `pg_publication_tables`, but with some minor tweaks (COALESCE, only return the rowfilter,
        // filter on oid and pubname). All of these are available >= Postgres 15.
        let row_filter_query = format!(
            "select pt.rowfilter as row_filter 
                from pg_publication_tables pt 
                join pg_namespace n on n.nspname = pt.schemaname 
                join pg_class c on c.relnamespace = n.oid AND c.relname = pt.tablename 
                where pt.pubname = {} and c.oid = {};",
            quote_literal(publication),
            table_id,
        );

        let row_filters = self.client.simple_query(&row_filter_query).await?;

        for row_filter in row_filters {
            if let SimpleQueryMessage::Row(row) = row_filter {
                let row_filter = row.try_get("row_filter")?;
                match row_filter {
                    None => return Ok(None),
                    Some(row_filter) => return Ok(Some(row_filter.to_string())),
                }
            }
        }

        Ok(None)
    }

    /// Creates a COPY stream for reading data from a table using its OID.
    ///
    /// The stream will include only the specified columns and use text format, and respect publication row filters (if a publication is specified)
    pub async fn get_table_copy_stream(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication: Option<&str>,
    ) -> EtlResult<CopyOutStream> {
        let column_list = column_schemas
            .iter()
            .map(|col| quote_identifier(&col.name))
            .collect::<Vec<_>>()
            .join(", ");

        let table_name = self.get_table_name(table_id).await?;
        let row_filter = self.get_row_filter(table_id, publication).await?;

        let copy_query = if let Some(row_filter) = row_filter {
            // Use select-form so we can add where.
            format!(
                r#"copy (select {} from {} where {}) to stdout with (format text);"#,
                column_list,
                table_name.as_quoted_identifier(),
                row_filter,
            )
        } else {
            format!(
                r#"copy (select {} from {}) to stdout with (format text);"#,
                column_list,
                table_name.as_quoted_identifier(),
            )
        };

        let stream = self.client.copy_out_simple(&copy_query).await?;

        Ok(stream)
    }

    /// Creates a COPY stream for a ctid partition range of the specified table.
    ///
    /// Resolves the table name and row filter internally, then streams rows whose ctid
    /// falls within the given partition bounds.
    async fn get_table_copy_stream_with_ctid_partition(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
        partition: &CtidPartition,
    ) -> EtlResult<CopyOutStream> {
        let table_name = self.get_table_name(table_id).await?;
        let row_filter = self.get_row_filter(table_id, publication_name).await?;

        let column_list = column_schemas
            .iter()
            .map(|col| quote_identifier(&col.name))
            .collect::<Vec<_>>()
            .join(", ");

        let query = Self::build_ctid_copy_query(
            &table_name,
            &column_list,
            row_filter.as_deref(),
            partition,
        );

        let stream = self.client.copy_out_simple(&query).await?;

        Ok(stream)
    }

    /// Builds a `COPY ... TO STDOUT` query that selects rows within a ctid range.
    ///
    /// The query applies an optional publication row filter in addition to the ctid bounds.
    fn build_ctid_copy_query(
        table_name: &TableName,
        column_list: &str,
        row_filter: Option<&str>,
        partition: &CtidPartition,
    ) -> String {
        let ctid_predicate = match partition {
            CtidPartition::OpenStart { end_tid } => {
                format!("ctid < {}::tid", quote_literal(end_tid))
            }
            CtidPartition::Closed { start_tid, end_tid } => {
                format!(
                    "ctid >= {}::tid and ctid < {}::tid",
                    quote_literal(start_tid),
                    quote_literal(end_tid),
                )
            }
            CtidPartition::OpenEnd { start_tid } => {
                format!("ctid >= {}::tid", quote_literal(start_tid))
            }
        };

        if let Some(row_filter) = row_filter {
            format!(
                "copy (select {column_list} from {table_name} where {ctid_predicate} and ({row_filter})) to stdout with (format text);",
            )
        } else {
            format!(
                "copy (select {column_list} from {table_name} where {ctid_predicate}) to stdout with (format text);",
            )
        }
    }

    /// Exports the current transaction snapshot.
    ///
    /// Calls `pg_export_snapshot()` so that child connections can use
    /// `SET TRANSACTION SNAPSHOT` to read from the same consistent point.
    async fn export_snapshot(&self) -> EtlResult<String> {
        let results = self
            .client
            .simple_query("select pg_export_snapshot();")
            .await?;

        for msg in results {
            if let SimpleQueryMessage::Row(row) = msg {
                let snapshot_id = row
                    .try_get(0)?
                    .ok_or_else(|| {
                        etl_error!(ErrorKind::InvalidState, "pg_export_snapshot returned NULL")
                    })?
                    .to_string();
                return Ok(snapshot_id);
            }
        }

        Err(etl_error!(
            ErrorKind::InvalidState,
            "pg_export_snapshot returned no rows"
        ))
    }

    /// Computes balanced ctid partition ranges using relation-size-based blocks.
    ///
    /// Returns one [`CtidPartition`] per partition, or an empty vec if the table has no rows.
    ///
    /// This method divides the table into roughly equal physical ranges based on block
    /// numbers to avoid a full table scan and sort.
    /// The approach:
    /// 1. Queries `pg_relation_size(table)` and divides by `current_setting('block_size')`
    /// 2. Divides blocks evenly across `num_partitions`
    /// 3. Generates half-open ctid ranges on block boundaries
    ///
    /// The final partition uses an open upper bound to include all blocks at or above its
    /// lower boundary, which keeps coverage correct when the table grows after size sampling.
    ///
    /// The size calculation is not snapshot based, which is why we have open intervals since we want
    /// to be sure to be able to capture all tuples and not miss anything.
    async fn plan_ctid_partitions(
        &self,
        table_id: TableId,
        num_partitions: u16,
    ) -> EtlResult<Vec<CtidPartition>> {
        if num_partitions == 0 {
            return Err(etl_error!(
                ErrorKind::ConfigError,
                "Number of ctid partitions must be greater than zero"
            ));
        }

        // We query how many blocks the table has at this point in time. Note that this query doesn't
        // use MVCC, so it's a real-time snapshot.
        let size_query = format!(
            "select \
                 pg_relation_size({table_id}::regclass)::bigint / current_setting('block_size')::bigint as table_blocks"
        );
        let size_results = self.client.simple_query(&size_query).await?;
        let table_blocks: i64 = size_results
            .iter()
            .find_map(|msg| {
                if let SimpleQueryMessage::Row(row) = msg {
                    row.try_get("table_blocks").ok()??.parse().ok()
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::SourceSchemaError,
                    "Could not retrieve table block count for partition planning",
                    format!("table_id: {table_id}")
                )
            })?;

        if table_blocks == 0 {
            return Ok(vec![]);
        }

        let requested_partitions = i64::from(num_partitions);
        let effective_partitions = requested_partitions.min(table_blocks);
        // We perform ceil-division with the classic formula to avoid having undersized partitions.
        let blocks_per_partition = (table_blocks + effective_partitions - 1) / effective_partitions;

        let mut partitions = Vec::with_capacity(effective_partitions as usize);
        for i in 0..effective_partitions {
            let start_block = i * blocks_per_partition;
            // We use the next block as exclusive delimiter for the query to avoid possible issues
            // in the way we determine boundaries.
            let end_block_exclusive = ((i + 1) * blocks_per_partition).min(table_blocks);

            let partition = if effective_partitions == 1 {
                CtidPartition::OpenEnd {
                    start_tid: "(0,1)".to_string(),
                }
            } else if i == 0 {
                CtidPartition::OpenStart {
                    end_tid: format!("({end_block_exclusive},1)"),
                }
            } else if i == effective_partitions - 1 {
                CtidPartition::OpenEnd {
                    start_tid: format!("({start_block},1)"),
                }
            } else {
                CtidPartition::Closed {
                    start_tid: format!("({start_block},1)"),
                    end_tid: format!("({end_block_exclusive},1)"),
                }
            };

            partitions.push(partition);
        }

        Ok(partitions)
    }

    /// Helper function to extract a value from a SimpleQueryMessage::Row
    ///
    /// Returns an error if the column is not found or if the value cannot be parsed to the target type.
    async fn get_row_value<T: std::str::FromStr>(
        row: &SimpleQueryRow,
        column_name: &str,
        table_name: &str,
    ) -> EtlResult<T>
    where
        T::Err: fmt::Debug,
    {
        let value = row.try_get(column_name)?.ok_or(etl_error!(
            ErrorKind::SourceSchemaError,
            "Column not found in source table",
            format!(
                "Column '{}' not found in table '{}'",
                column_name, table_name
            )
        ))?;

        value.parse().map_err(|e: T::Err| {
            etl_error!(
                ErrorKind::ConversionError,
                "Column parsing failed",
                format!(
                    "Failed to parse value from column '{}' in table '{}': {:?}",
                    column_name, table_name, e
                )
            )
        })
    }
}
