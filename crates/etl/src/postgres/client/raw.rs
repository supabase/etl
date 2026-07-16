use std::{fmt, num::NonZeroI32, sync::Arc, time::Duration};

use etl_postgres::{
    application_name::{apply_worker_application_name, table_sync_worker_application_name},
    source::extract_server_version,
    tokio::tls::MakeRustlsConnect,
};
use pg_escape::{quote_identifier, quote_literal};
use postgres_replication::LogicalReplicationStream;
use rustls::{
    ClientConfig,
    pki_types::{CertificateDer, pem::PemObject},
};
use tokio::sync::watch;
use tokio_postgres::{
    Client, Config, Connection, IsolationLevel, NoTls, SimpleQueryMessage, Socket, Transaction,
    config::ReplicationMode, error::SqlState, tls::MakeTlsConnect, types::PgLsn,
};
use tracing::{Instrument, debug, error, info};

use super::{
    child::ChildPgReplicationClient,
    query::PgReplicationQueryTarget,
    transaction::PgReplicationTransaction,
    types::{
        CreateSlotResult, GetOrCreateSlotResult, GetSlotResult, PostgresConnectionUpdate,
        SlotState, SnapshotAction,
    },
    utils::get_row_value,
};
use crate::{
    bail,
    config::{IntoConnectOptions, PgConnectionConfig, PgConnectionOptions},
    error::{ErrorKind, EtlResult},
    etl_error,
    pipeline::PipelineId,
    schema::{TableId, TableName},
};

/// Maximum time to wait for a replication slot deletion to complete.
///
/// Slot deletion uses `WAIT`, which can block until the slot is no longer in
/// use. This timeout ensures calls are bounded and cannot wait forever.
const DELETE_SLOT_TIMEOUT: Duration = Duration::from_secs(30);
/// Default duration unit used when `pg_settings.unit` is empty.
const PG_SETTINGS_DEFAULT_DURATION_UNIT: &str = "ms";
/// Base application name for ETL logical replication connections.
///
/// Worker connections append a suffix (see [`etl_postgres::application_name`])
/// so they can be told apart in `pg_stat_activity`.
pub const APP_NAME_REPLICATOR_REPLICATION: &str = "supabase_etl_replicator_replication";

/// Builds connection options for logical replication connections.
///
/// Disables statement, lock, and idle-in-transaction timeouts because
/// replication streams, slot creation, and initial table synchronization can
/// legitimately run for a long time.
fn replication_options(application_name: String) -> PgConnectionOptions {
    PgConnectionOptions::builder(application_name)
        .statement_timeout(0)
        .lock_timeout(0)
        .idle_in_transaction_session_timeout(0)
        .build()
}

/// The kind of PostgreSQL connection to create.
#[derive(Debug, Clone, Copy)]
enum ConnectionKind {
    /// A logical replication connection.
    Replication,
    /// A normal SQL connection used for snapshot-sharing child work.
    Child,
}

impl ConnectionKind {
    /// Configures the PostgreSQL connection options for this connection kind.
    fn configure(self, config: &mut Config) {
        if matches!(self, Self::Replication) {
            config.replication_mode(ReplicationMode::Logical);
        }
    }
}

/// Builds a rustls client config from PEM-encoded trusted root certificates.
fn tls_client_config_from_root_certs(trusted_root_certs: &str) -> EtlResult<ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(trusted_root_certs.as_bytes()) {
        let cert = cert?;
        root_store.add(cert)?;
    }

    Ok(ClientConfig::builder().with_root_certificates(root_store).with_no_client_auth())
}

/// Shared connection settings used to create parent and child replication
/// connections.
///
/// The raw [`PgConnectionConfig`] can contain large PEM certificate strings, so
/// this wrapper keeps it behind an [`Arc`] and caches the parsed rustls
/// [`ClientConfig`] for TLS connections.
#[derive(Clone)]
pub(super) struct PgReplicationConnectionConfig {
    /// Original PostgreSQL connection settings.
    pg_connection_config: Arc<PgConnectionConfig>,
    /// Session options, including the per-worker `application_name`.
    ///
    /// Child connections reuse these options, so they inherit the parent
    /// worker's `application_name`.
    options: Arc<PgConnectionOptions>,
    /// Parsed rustls client config, present when TLS is enabled.
    tls_client_config: Option<Arc<ClientConfig>>,
}

impl PgReplicationConnectionConfig {
    /// Creates shared connection settings from an owned PostgreSQL config.
    fn new(
        pg_connection_config: PgConnectionConfig,
        options: PgConnectionOptions,
    ) -> EtlResult<Self> {
        let tls_client_config = if pg_connection_config.tls.enabled {
            Some(Arc::new(tls_client_config_from_root_certs(
                &pg_connection_config.tls.trusted_root_certs,
            )?))
        } else {
            None
        };

        Ok(Self {
            pg_connection_config: Arc::new(pg_connection_config),
            options: Arc::new(options),
            tls_client_config,
        })
    }

    /// Returns the raw PostgreSQL connection settings.
    fn pg_connection_config(&self) -> &PgConnectionConfig {
        &self.pg_connection_config
    }

    /// Returns the session options for connections created from this config.
    fn options(&self) -> &PgConnectionOptions {
        &self.options
    }

    /// Returns the shared rustls client config.
    fn tls_client_config(&self) -> Option<Arc<ClientConfig>> {
        self.tls_client_config.as_ref().map(Arc::clone)
    }
}

impl fmt::Debug for PgReplicationConnectionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.pg_connection_config();

        f.debug_struct("PgReplicationConnectionConfig")
            .field("host_configured", &!config.host.is_empty())
            .field("hostaddr_configured", &config.hostaddr.is_some())
            .field("port", &config.port)
            .field("database_configured", &!config.name.is_empty())
            .field("username_configured", &!config.username.is_empty())
            .field("tls_enabled", &config.tls.enabled)
            .field("tls_client_configured", &self.tls_client_config.is_some())
            .finish()
    }
}

/// Spawns a background task to monitor a Postgres connection until it
/// terminates.
fn spawn_postgres_connection<T>(
    connection: Connection<Socket, T::Stream>,
) -> watch::Receiver<PostgresConnectionUpdate>
where
    T: MakeTlsConnect<Socket>,
    T::Stream: Send + 'static,
{
    // We use this watch channel to send connection updates without relying on the
    // errors/terminations propagated by the active connection consumers.
    let (updates_tx, updates_rx) = watch::channel(PostgresConnectionUpdate::Running);

    let span = tracing::Span::current();
    let task = async move {
        let result = connection.await;

        match result {
            Err(err) => {
                let error_message = err.to_string();
                let _ = updates_tx.send(PostgresConnectionUpdate::errored(error_message.clone()));
                error!(error = %error_message, "postgres connection error");
            }
            Ok(()) => {
                let _ = updates_tx.send(PostgresConnectionUpdate::Terminated);
                info!("postgres connection terminated");
            }
        }
    }
    .instrument(span);

    // There is no need to track the connection task via the `JoinHandle` since the
    // `Client`, which returned the connection, will automatically terminate the
    // connection when dropped.
    tokio::spawn(task);

    updates_rx
}

/// A client for interacting with Postgres's logical replication features.
///
/// This client provides methods for creating replication slots, managing
/// transactions, and streaming changes from the database.
///
/// The client owns a single PostgreSQL connection and is intentionally not
/// cloneable. Methods that open a transaction borrow the client mutably, so no
/// other command can be issued on the same connection while that transaction is
/// alive.
pub struct PgReplicationClient {
    client: Client,
    connection_config: PgReplicationConnectionConfig,
    server_version: Option<NonZeroI32>,
    connection_updates_rx: watch::Receiver<PostgresConnectionUpdate>,
}

impl fmt::Debug for PgReplicationClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgReplicationClient")
            .field("connection_config", &self.connection_config)
            .field("server_version", &self.server_version)
            .field("is_closed", &self.client.is_closed())
            .finish()
    }
}

impl PgReplicationClient {
    /// Establishes a connection to Postgres. The connection uses TLS if
    /// configured in the supplied [`PgConnectionConfig`].
    ///
    /// The connection is configured for logical replication mode and uses the
    /// base replication `application_name`. Worker connections should use
    /// [`PgReplicationClient::connect_for_apply_worker`] or
    /// [`PgReplicationClient::connect_for_table_sync_worker`] instead.
    pub async fn connect(pg_connection_config: PgConnectionConfig) -> EtlResult<Self> {
        Self::connect_with_application_name(
            pg_connection_config,
            APP_NAME_REPLICATOR_REPLICATION.to_owned(),
        )
        .await
    }

    /// Establishes a replication connection tagged for an apply worker.
    pub async fn connect_for_apply_worker(
        pg_connection_config: PgConnectionConfig,
        pipeline_id: PipelineId,
    ) -> EtlResult<Self> {
        Self::connect_with_application_name(
            pg_connection_config,
            apply_worker_application_name(APP_NAME_REPLICATOR_REPLICATION, pipeline_id),
        )
        .await
    }

    /// Establishes a replication connection tagged for a table sync worker.
    pub async fn connect_for_table_sync_worker(
        pg_connection_config: PgConnectionConfig,
        pipeline_id: PipelineId,
        table_id: TableId,
    ) -> EtlResult<Self> {
        Self::connect_with_application_name(
            pg_connection_config,
            table_sync_worker_application_name(
                APP_NAME_REPLICATOR_REPLICATION,
                pipeline_id,
                table_id,
            ),
        )
        .await
    }

    /// Establishes a replication connection with the supplied
    /// `application_name`.
    async fn connect_with_application_name(
        pg_connection_config: PgConnectionConfig,
        application_name: String,
    ) -> EtlResult<Self> {
        let options = replication_options(application_name);
        let connection_config = PgReplicationConnectionConfig::new(pg_connection_config, options)?;

        PgReplicationClient::connect_with_kind(connection_config, ConnectionKind::Replication).await
    }

    /// Establishes a connection to Postgres using the supplied connection kind.
    async fn connect_with_kind(
        connection_config: PgReplicationConnectionConfig,
        kind: ConnectionKind,
    ) -> EtlResult<Self> {
        match connection_config.pg_connection_config().tls.enabled {
            true => PgReplicationClient::connect_tls(connection_config, kind).await,
            false => PgReplicationClient::connect_no_tls(connection_config, kind).await,
        }
    }

    /// Establishes a connection to Postgres without TLS encryption.
    async fn connect_no_tls(
        connection_config: PgReplicationConnectionConfig,
        kind: ConnectionKind,
    ) -> EtlResult<Self> {
        let mut config: Config =
            connection_config.pg_connection_config().with_db(Some(connection_config.options()));
        kind.configure(&mut config);

        let (client, connection) = config.connect(NoTls).await?;

        let server_version =
            connection.parameter("server_version").and_then(extract_server_version);

        let connection_updates_rx = spawn_postgres_connection::<NoTls>(connection);

        info!("connected to postgres without tls");

        Ok(PgReplicationClient { client, connection_config, server_version, connection_updates_rx })
    }

    /// Establishes a TLS-encrypted connection to Postgres.
    async fn connect_tls(
        connection_config: PgReplicationConnectionConfig,
        kind: ConnectionKind,
    ) -> EtlResult<Self> {
        let mut config: Config =
            connection_config.pg_connection_config().with_db(Some(connection_config.options()));
        kind.configure(&mut config);

        let tls_config = connection_config.tls_client_config().ok_or_else(|| {
            etl_error!(ErrorKind::InvalidState, "TLS client config missing for TLS connection")
        })?;
        let (client, connection) =
            config.connect(MakeRustlsConnect::from_shared_config(tls_config)).await?;

        let server_version =
            connection.parameter("server_version").and_then(extract_server_version);

        let connection_updates_rx = spawn_postgres_connection::<MakeRustlsConnect>(connection);

        info!("connected to postgres with tls");

        Ok(PgReplicationClient { client, connection_config, server_version, connection_updates_rx })
    }

    /// Creates a non-replication child connection from connection settings.
    pub(super) async fn connect_child_from_config(
        connection_config: PgReplicationConnectionConfig,
    ) -> EtlResult<ChildPgReplicationClient> {
        let client = Self::connect_with_kind(connection_config, ConnectionKind::Child).await?;

        Ok(ChildPgReplicationClient::new(client))
    }

    /// Checks if the underlying connection is closed.
    pub fn is_closed(&self) -> bool {
        self.client.is_closed()
    }

    /// Returns a receiver for background connection task updates.
    pub(crate) fn connection_updates_rx(&self) -> watch::Receiver<PostgresConnectionUpdate> {
        self.connection_updates_rx.clone()
    }

    /// Returns the server version reported by this connection.
    pub(super) fn server_version(&self) -> Option<NonZeroI32> {
        self.server_version
    }

    /// Returns the configured `wal_sender_timeout`, if PostgreSQL has it
    /// enabled.
    pub async fn get_wal_sender_timeout(&self) -> EtlResult<Option<Duration>> {
        let query = "select setting, unit from pg_settings where name = 'wal_sender_timeout';";

        for result in self.client.simple_query(query).await? {
            if let SimpleQueryMessage::Row(row) = result {
                let setting = get_row_value::<u64>(&row, "setting", "pg_settings")?;
                if setting == 0 {
                    return Ok(None);
                }

                let unit = row.try_get("unit")?.unwrap_or(PG_SETTINGS_DEFAULT_DURATION_UNIT);

                let duration = match unit {
                    "ms" => Duration::from_millis(setting),
                    "s" => Duration::from_secs(setting),
                    "min" => Duration::from_secs(setting.saturating_mul(60)),
                    "h" => Duration::from_secs(setting.saturating_mul(60 * 60)),
                    "d" => Duration::from_secs(setting.saturating_mul(60 * 60 * 24)),
                    unsupported_unit => {
                        return Err(etl_error!(
                            ErrorKind::ConversionError,
                            "Unsupported wal sender timeout unit",
                            format!(
                                "Unsupported wal_sender_timeout unit '{}' returned by pg_settings",
                                unsupported_unit
                            )
                        ));
                    }
                };

                return Ok(Some(duration));
            }
        }

        bail!(
            ErrorKind::SourceConnectionFailed,
            "Wal sender timeout not found",
            "The table pg_settings did not return wal_sender_timeout".to_owned()
        )
    }

    /// Creates a new logical replication slot with the specified name and a
    /// transaction pinned to the slot's snapshot.
    ///
    /// A `REPEATABLE READ` transaction is begun first, then the slot is created
    /// with `USE_SNAPSHOT` which pins the transaction to the slot's
    /// consistent snapshot. The transaction must be kept open for the
    /// duration of any operations that depend on this snapshot (e.g. schema
    /// fetches, table copies, or `pg_export_snapshot()` calls
    /// for child connections).
    pub async fn create_slot_with_transaction(
        &mut self,
        slot_name: &str,
    ) -> EtlResult<(PgReplicationTransaction<'_>, CreateSlotResult)> {
        let connection_config = self.connection_config.clone();
        let server_version = self.server_version;
        let connection_updates_rx = self.connection_updates_rx();

        let transaction = self.begin_tx().await?;
        let slot = PgReplicationQueryTarget::Transaction(&transaction)
            .create_slot(slot_name, SnapshotAction::Use)
            .await?;

        Ok((
            PgReplicationTransaction::new(
                transaction,
                connection_config,
                server_version,
                connection_updates_rx,
            ),
            slot,
        ))
    }

    /// Creates a new logical replication slot with the specified name and no
    /// snapshot.
    pub async fn create_slot(&self, slot_name: &str) -> EtlResult<CreateSlotResult> {
        self.create_slot_internal(slot_name, SnapshotAction::NoExport).await
    }

    /// Gets the state of a replication slot by name.
    ///
    /// Queries the `pg_replication_slots` system catalog to determine if the
    /// slot exists and whether it's valid or invalidated. A slot is
    /// considered invalidated when its `wal_status` is 'lost', indicating
    /// that required WAL segments have been removed.
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
                    Some(_) |
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
                let confirmed_flush_lsn =
                    get_row_value::<PgLsn>(&row, "confirmed_flush_lsn", "pg_replication_slots")?;
                let slot = GetSlotResult { confirmed_flush_lsn };

                return Ok(slot);
            }
        }

        bail!(
            ErrorKind::ReplicationSlotNotFound,
            "Replication slot not found",
            format!("Replication slot '{}' not found in database", slot_name)
        );
    }

    /// Gets an existing replication slot or creates a new one if it doesn't
    /// exist.
    ///
    /// This method first attempts to get the slot by name. If the slot doesn't
    /// exist, it creates a new one.
    ///
    /// Returns an enum indicating whether the slot was created or already
    /// existed.
    pub(crate) async fn get_or_create_slot(
        &self,
        slot_name: &str,
    ) -> EtlResult<GetOrCreateSlotResult> {
        match self.get_slot(slot_name).await {
            Ok(slot) => Ok(GetOrCreateSlotResult::GetSlot(slot)),
            Err(err) if err.kind() == ErrorKind::ReplicationSlotNotFound => {
                let create_result = self.create_slot(slot_name).await?;

                Ok(GetOrCreateSlotResult::CreateSlot(create_result))
            }
            Err(e) => Err(e),
        }
    }

    /// Deletes a replication slot with the specified name.
    ///
    /// Returns an error if the slot doesn't exist or if there are any issues
    /// with the deletion.
    pub async fn delete_slot(&self, slot_name: &str) -> EtlResult<()> {
        self.delete_slot_internal(slot_name, true).await
    }

    /// Deletes a replication slot with the specified name if it exists.
    ///
    /// This method returns `Ok(())` when the slot is missing and propagates
    /// any other error from [`PgReplicationClient::delete_slot`].
    pub async fn delete_slot_if_exists(&self, slot_name: &str) -> EtlResult<()> {
        self.delete_slot_internal(slot_name, false).await
    }

    /// Checks if a publication with the given name exists.
    pub async fn publication_exists(&self, publication_name: &str) -> EtlResult<bool> {
        let publication_exists_query = format!(
            "select 1 as exists from pg_publication where pubname = {};",
            quote_literal(publication_name)
        );
        for msg in self.client.simple_query(&publication_exists_query).await? {
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
                let schema = get_row_value::<String>(&row, "schemaname", "pg_publication_tables")?;
                let name = get_row_value::<String>(&row, "tablename", "pg_publication_tables")?;

                table_names.push(TableName { schema, name });
            }
        }

        Ok(table_names)
    }

    /// Retrieves the OIDs of all tables included in a publication.
    ///
    /// This follows `pg_get_publication_tables`, which applies PostgreSQL's
    /// logical replication identity rules for partitioned tables. With
    /// `publish_via_partition_root=true`, PostgreSQL returns the published
    /// partition root or subtree root used for relation messages. With
    /// `publish_via_partition_root=false`, it returns the leaf relations whose
    /// schemas are used for replication.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::ConfigError`] if the publication contains no
    /// tables. This typically indicates a misconfigured publication that
    /// won't replicate any data.
    pub async fn get_publication_table_ids(
        &self,
        publication_name: &str,
    ) -> EtlResult<Vec<TableId>> {
        let query = format!(
            r#"
            select distinct gpt.relid::oid as oid
            from pg_get_publication_tables({pub}) gpt
            order by oid;
            "#,
            pub = quote_literal(publication_name)
        );

        let mut table_ids = vec![];
        for row in self.client.simple_query(&query).await? {
            if let SimpleQueryMessage::Row(row) = row {
                let table_id = get_row_value::<TableId>(&row, "oid", "pg_class")?;
                table_ids.push(table_id);
            }
        }

        if table_ids.is_empty() {
            bail!(
                ErrorKind::ConfigError,
                "Publication has no tables",
                format!(
                    "Publication '{}' does not contain any tables. Ensure the publication is \
                     configured with tables using FOR TABLE, FOR ALL TABLES, or FOR TABLES IN \
                     SCHEMA.",
                    publication_name
                )
            );
        }

        Ok(table_ids)
    }

    /// Starts a logical replication stream from the specified publication and
    /// slot.
    ///
    /// The stream will begin reading changes from the provided `start_lsn`.
    pub async fn start_logical_replication(
        &self,
        publication_name: &str,
        slot_name: &str,
        start_lsn: PgLsn,
    ) -> EtlResult<LogicalReplicationStream> {
        info!(publication_name, slot_name, %start_lsn, "starting logical replication");

        // Do not convert the query or the options to lowercase, see comment in
        // `create_slot_internal`.
        let options = format!(
            r#"("proto_version" '1', "publication_names" {}, "messages" 'true')"#,
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
    /// The transaction doesn't make any assumptions about the snapshot in use,
    /// since this is a concern of the statements issued within the
    /// transaction.
    pub(super) async fn begin_tx(&mut self) -> EtlResult<Transaction<'_>> {
        let transaction = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;

        Ok(transaction)
    }

    /// Returns this client as a query target.
    fn target(&self) -> PgReplicationQueryTarget<'_, '_> {
        PgReplicationQueryTarget::Client(&self.client)
    }

    /// Internal helper method to create a replication slot.
    ///
    /// The `snapshot_action` controls how the slot's snapshot is handled during
    /// creation.
    async fn create_slot_internal(
        &self,
        slot_name: &str,
        snapshot_action: SnapshotAction,
    ) -> EtlResult<CreateSlotResult> {
        self.target().create_slot(slot_name, snapshot_action).await
    }

    /// Deletes a replication slot, optionally failing when the slot does not
    /// exist.
    async fn delete_slot_internal(&self, slot_name: &str, fail_if_missing: bool) -> EtlResult<()> {
        debug!(slot_name, "deleting replication slot");

        // Do not convert the query or the options to lowercase, see comment in
        // `create_slot_internal`.
        let query = format!(r#"DROP_REPLICATION_SLOT {} WAIT;"#, quote_identifier(slot_name));

        let Ok(delete_result) =
            tokio::time::timeout(DELETE_SLOT_TIMEOUT, self.client.simple_query(&query)).await
        else {
            bail!(
                ErrorKind::ReplicationSlotDeletionTimeout,
                "Replication slot deletion timed out",
                format!(
                    "Timed out after {:?} while deleting replication slot '{}'",
                    DELETE_SLOT_TIMEOUT, slot_name
                )
            )
        };

        match delete_result {
            Ok(_) => {
                info!(slot_name, "deleted replication slot");

                Ok(())
            }
            Err(err) => {
                if let Some(&SqlState::UNDEFINED_OBJECT) = err.code() {
                    if fail_if_missing {
                        bail!(
                            ErrorKind::ReplicationSlotNotFound,
                            "Replication slot not found",
                            format!(
                                "Replication slot '{}' not found in database while attempting its \
                                 deletion",
                                slot_name
                            )
                        );
                    }

                    info!(slot_name, "replication slot not found, skipping deletion");

                    return Ok(());
                }

                Err(err.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replication_base_name_fits_worker_suffix_without_clamping() {
        // --- GIVEN: a worker suffix with pipeline id and table oid summing to 15
        // digits, the documented no-clamp bound for the replication base name ---
        let name = table_sync_worker_application_name(
            APP_NAME_REPLICATOR_REPLICATION,
            99_999,
            TableId::new(u32::MAX),
        );

        // --- THEN: the name fits Postgres's 63-byte limit with the base intact ---
        assert!(name.len() <= 63, "worker application_name '{name}' exceeds Postgres limit");
        assert!(name.starts_with(APP_NAME_REPLICATOR_REPLICATION));
    }
}
