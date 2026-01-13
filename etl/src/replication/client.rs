use crate::error::{ErrorKind, EtlResult};
use crate::utils::tokio::MakeRustlsConnect;
use crate::{bail, etl_error};
use etl_config::shared::{ETL_HEARTBEAT_OPTIONS, ETL_REPLICATION_OPTIONS, IntoConnectOptions, PgConnectionConfig};
use etl_postgres::replication::extract_server_version;
use etl_postgres::types::convert_type_oid_to_type;
use etl_postgres::types::{ColumnSchema, TableId, TableName, TableSchema};
use postgres_replication::LogicalReplicationStream;
use rustls::ClientConfig;
use rustls::pki_types::CertificateDer;
use std::collections::HashMap;
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Config, NoTls};
use tracing::{debug, info, instrument, warn};

const MIN_PG_VERSION: i32 = 14_00_00;
const MAX_PG_VERSION: i32 = 18_00_00;

pub(crate) struct ConnectionDetails {
    pub(crate) conn_string: String,
    pub(crate) version: i32,
}

pub struct PgReplicationClient {
    client: Client,
    pub(crate) conn_details: ConnectionDetails,
}

/// Spawns a background task to handle the Postgres connection.
///
/// This is required by tokio-postgres to properly handle asynchronous communication.
fn spawn_postgres_connection<T>(
    connection: tokio_postgres::Connection<tokio_postgres::Socket, T>,
) where
    T: tokio_postgres::tls::TlsStream + std::marker::Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {}", e);
        }
    });
}

impl PgReplicationClient {
    /// Establishes a connection to Postgres with logical replication mode enabled.
    ///
    /// Validates that the server version is within the supported range (PG 14-18).
    pub async fn connect(pg_connection_config: PgConnectionConfig) -> EtlResult<Self> {
        match pg_connection_config.tls.enabled {
            true => Self::connect_tls(pg_connection_config).await,
            false => Self::connect_no_tls(pg_connection_config).await,
        }
    }

    /// Builds a connection string for display and identification purposes.
    ///
    /// This string does NOT include credentials and is safe for logging.
    fn build_conn_string(pg_connection_config: &PgConnectionConfig) -> String {
        format!(
            "{}@{}:{}/{}",
            pg_connection_config.username,
            pg_connection_config.host,
            pg_connection_config.port,
            pg_connection_config.name
        )
    }

    /// Validates that the Postgres server version is within supported bounds.
    fn validate_server_version(version: i32, conn_details: &ConnectionDetails) -> EtlResult<()> {
        if version < MIN_PG_VERSION || version >= MAX_PG_VERSION {
            bail!(
                ErrorKind::PostgresConnection,
                "unsupported PostgreSQL version: {}. ETL supports PostgreSQL versions 14 to 17. Server: {}",
                version,
                conn_details.conn_string
            );
        }
        Ok(())
    }

    /// Creates a new PgReplicationClient from a connected client.
    async fn new_from_client(
        client: Client,
        pg_connection_config: &PgConnectionConfig,
    ) -> EtlResult<Self> {
        let version = extract_server_version(&client).await?;
        let conn_string = Self::build_conn_string(pg_connection_config);

        let conn_details = ConnectionDetails {
            conn_string,
            version,
        };

        Self::validate_server_version(version, &conn_details)?;

        info!(
            pg_version = version,
            "connected to postgres for replication"
        );

        Ok(Self {
            client,
            conn_details,
        })
    }

    /// Builds a TLS root certificate store from PEM-encoded certificates.
    fn build_root_cert_store(pem_certs: &str) -> EtlResult<rustls::RootCertStore> {
        let mut root_store = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(pem_certs.as_bytes()) {
            let cert = cert?;
            root_store.add(cert)?;
        }
        Ok(root_store)
    }

    /// Establishes a regular (non-replication) connection to Postgres.
    ///
    /// This is used for operations that don't require logical replication mode,
    /// such as heartbeat emissions. Uses ETL_HEARTBEAT_OPTIONS for connection settings.
    pub async fn connect_regular(pg_connection_config: PgConnectionConfig) -> EtlResult<Client> {
        match pg_connection_config.tls.enabled {
            true => Self::connect_regular_tls(pg_connection_config).await,
            false => Self::connect_regular_no_tls(pg_connection_config).await,
        }
    }

    /// Establishes a regular connection to Postgres without TLS encryption.
    async fn connect_regular_no_tls(pg_connection_config: PgConnectionConfig) -> EtlResult<Client> {
        let config: Config = pg_connection_config
            .clone()
            .with_db(Some(&ETL_HEARTBEAT_OPTIONS));
        // Note: NO replication_mode() call - this is a regular connection.

        let (client, connection) = config.connect(NoTls).await?;
        spawn_postgres_connection::<NoTls>(connection);

        info!("connected to postgres (regular mode) without tls");
        Ok(client)
    }

    /// Establishes a regular TLS-encrypted connection to Postgres.
    async fn connect_regular_tls(pg_connection_config: PgConnectionConfig) -> EtlResult<Client> {
        let config: Config = pg_connection_config
            .clone()
            .with_db(Some(&ETL_HEARTBEAT_OPTIONS));
        // Note: NO replication_mode() call - this is a regular connection.

        let root_store = Self::build_root_cert_store(&pg_connection_config.tls.trusted_root_certs)?;

        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let (client, connection) = config.connect(MakeRustlsConnect::new(tls_config)).await?;
        spawn_postgres_connection::<MakeRustlsConnect>(connection);

        info!("connected to postgres (regular mode) with tls");
        Ok(client)
    }

    /// Establishes a connection to Postgres without TLS encryption.
    ///
    /// The connection is configured for logical replication mode.
    async fn connect_no_tls(pg_connection_config: PgConnectionConfig) -> EtlResult<Self> {
        let config: Config = pg_connection_config
            .clone()
            .with_db(Some(&ETL_REPLICATION_OPTIONS));
        let config = config.replication_mode(ReplicationMode::Logical);

        let (client, connection) = config.connect(NoTls).await?;
        spawn_postgres_connection::<NoTls>(connection);

        info!("connected to postgres without tls");
        Self::new_from_client(client, &pg_connection_config).await
    }

    /// Establishes a TLS-encrypted connection to Postgres.
    ///
    /// The connection is configured for logical replication mode.
    async fn connect_tls(pg_connection_config: PgConnectionConfig) -> EtlResult<Self> {
        let config: Config = pg_connection_config
            .clone()
            .with_db(Some(&ETL_REPLICATION_OPTIONS));
        let config = config.replication_mode(ReplicationMode::Logical);

        let root_store = Self::build_root_cert_store(&pg_connection_config.tls.trusted_root_certs)?;

        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let (client, connection) = config.connect(MakeRustlsConnect::new(tls_config)).await?;
        spawn_postgres_connection::<MakeRustlsConnect>(connection);

        info!("connected to postgres with tls");
        Self::new_from_client(client, &pg_connection_config).await
    }

    /// Queries table schemas for tables in the specified publication.
    ///
    /// Returns a vector of [`TableSchema`] containing column definitions and type information.
    #[instrument(skip(self))]
    pub async fn get_publication_table_schemas(
        &self,
        publication_name: &str,
    ) -> EtlResult<Vec<TableSchema>> {
        let query = r#"
            SELECT
                c.oid AS table_id,
                n.nspname AS schema_name,
                c.relname AS table_name,
                a.attnum AS column_position,
                a.attname AS column_name,
                CASE
                    WHEN t.typtype = 'd' THEN t.typbasetype
                    ELSE a.atttypid
                END AS type_oid,
                CASE
                    WHEN t.typtype = 'd' THEN
                        (SELECT bt.typname FROM pg_type bt WHERE bt.oid = t.typbasetype)
                    ELSE t.typname
                END AS type_name,
                CASE
                    WHEN a.atttypmod > 0 THEN a.atttypmod
                    WHEN t.typtype = 'd' AND t.typtypmod > 0 THEN t.typtypmod
                    ELSE -1
                END AS type_modifier
            FROM pg_publication p
            JOIN pg_publication_rel pr ON p.oid = pr.prpubid
            JOIN pg_class c ON pr.prrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_attribute a ON c.oid = a.attrelid
            JOIN pg_type t ON a.atttypid = t.oid
            WHERE
                p.pubname = $1
                AND a.attnum > 0
                AND NOT a.attisdropped
            ORDER BY c.oid, a.attnum
        "#;

        let rows = self.client.query(query, &[&publication_name]).await?;

        let mut tables: HashMap<TableId, TableSchema> = HashMap::new();

        for row in rows {
            let table_id: TableId = TableId::new(row.get::<_, u32>("table_id"));
            let schema_name: String = row.get("schema_name");
            let table_name: String = row.get("table_name");
            let column_position: i16 = row.get("column_position");
            let column_name: String = row.get("column_name");
            let type_oid: u32 = row.get("type_oid");
            let type_name: String = row.get("type_name");
            let type_modifier: i32 = row.get("type_modifier");

            // Convert OID to Type, falling back to TEXT for unsupported types.
            let pg_type = convert_type_oid_to_type(type_oid);
            let pg_type = match pg_type {
                Some(t) => t,
                None => {
                    warn!(
                        table_id = %table_id,
                        column_name = %column_name,
                        type_name = %type_name,
                        type_oid = %type_oid,
                        "unsupported column type, treating as TEXT"
                    );
                    Type::TEXT
                }
            };

            let column = ColumnSchema {
                position: column_position as usize,
                name: column_name.clone(),
                pg_type,
                type_modifier,
            };

            tables
                .entry(table_id)
                .or_insert_with(|| TableSchema {
                    id: table_id,
                    name: TableName::new(&schema_name, &table_name),
                    columns: Vec::new(),
                })
                .columns
                .push(column);
        }

        debug!(
            table_count = tables.len(),
            "discovered publication table schemas"
        );

        Ok(tables.into_values().collect())
    }

    /// Creates a logical replication slot if it doesn't already exist.
    ///
    /// Uses pgoutput as the output plugin for logical decoding.
    #[instrument(skip(self))]
    pub async fn create_replication_slot_if_not_exists(
        &self,
        slot_name: &str,
    ) -> EtlResult<Option<String>> {
        // First check if the slot already exists.
        let check_query = "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1";
        let existing_slot = self.client.query_opt(check_query, &[&slot_name]).await?;

        if let Some(row) = existing_slot {
            let lsn: Option<String> = row.get("confirmed_flush_lsn");
            info!(slot_name = %slot_name, lsn = ?lsn, "replication slot already exists");
            return Ok(lsn);
        }

        // Create new slot.
        let create_query = format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput",
            slot_name
        );
        self.client.simple_query(&create_query).await?;

        info!(slot_name = %slot_name, "created new replication slot");
        Ok(None)
    }

    /// Starts streaming logical replication from the specified LSN.
    ///
    /// Returns a [`LogicalReplicationStream`] that yields replication messages.
    #[instrument(skip(self))]
    pub async fn start_replication(
        self,
        slot_name: &str,
        publication_name: &str,
        start_lsn: &str,
    ) -> EtlResult<LogicalReplicationStream> {
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (\"proto_version\" '2', \"publication_names\" '{}', \"messages\" 'true', \"streaming\" 'true')",
            slot_name, start_lsn, publication_name
        );

        let copy_stream = self.client.copy_both_simple::<bytes::Bytes>(&query).await?;
        let stream = LogicalReplicationStream::new(copy_stream);

        info!(
            slot_name = %slot_name,
            publication_name = %publication_name,
            start_lsn = %start_lsn,
            "started logical replication stream"
        );

        Ok(stream)
    }

    /// Executes a COPY query to bulk export table data.
    ///
    /// Returns a stream of binary-encoded rows for efficient data transfer.
    #[instrument(skip(self))]
    pub async fn copy_table(
        &self,
        table_name: &TableName,
    ) -> EtlResult<tokio_postgres::CopyOutStream> {
        let query = format!(
            "COPY {} TO STDOUT (FORMAT binary)",
            table_name.full_name
        );

        let stream = self.client.copy_out(&query).await?;

        debug!(table = %table_name.full_name, "started table copy stream");

        Ok(stream)
    }
}
