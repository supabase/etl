use crate::error::{ErrorKind, EtlResult};
use crate::utils::tokio::MakeRustlsConnect;
use crate::{bail, etl_error};
use etl_config::shared::{IntoConnectOptions, PgConnectionConfig};
use etl_postgres::below_version;
use etl_postgres::replication::extract_server_version;
use etl_postgres::types::convert_type_oid_to_type;
use etl_postgres::types::{ColumnSchema, TableId, TableName, TableSchema};
use etl_postgres::version::POSTGRES_15;
use pg_escape::{quote_identifier, quote_literal};
use postgres_replication::LogicalReplicationStream;
use rustls::ClientConfig;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::BufReader;
use std::num::NonZeroI32;
use std::sync::Arc;

use tokio_postgres::error::SqlState;
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{
    Client, Config, Connection, CopyOutStream, NoTls, SimpleQueryMessage, SimpleQueryRow, Socket,
    config::ReplicationMode, types::PgLsn,
};
use tracing::{Instrument, error, info, warn};

/// Spawns a background task to monitor a Postgres connection until it terminates.
fn spawn_postgres_connection<T>(connection: Connection<Socket, T::Stream>)
where
    T: MakeTlsConnect<Socket>,
    T::Stream: Send + 'static,
{
    let span = tracing::Span::current();
    let task = async move {
        let result = connection.await;

        match result {
            Err(err) => error!("an error occurred during the postgres connection: {}", err),
            Ok(()) => info!("postgres connection terminated successfully"),
        }
    }
    .instrument(span);

    // There is no need to track the connection task via the `JoinHandle` since the `Client`, which
    // returned the connection, will automatically terminate the connection when dropped.
    tokio::spawn(task);
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

/// A transaction that operates within the context of a replication slot.
///
/// This type ensures that the parent connection remains active for the duration of any
/// transaction spawned by that connection for a given slot.
///
/// The `client` is the client that created the slot and must be active for the duration of
/// the transaction for the snapshot of the slot to be consistent.
#[derive(Debug)]
pub struct PgReplicationSlotTransaction {
    client: PgReplicationClient,
}

impl PgReplicationSlotTransaction {
    /// Creates a new transaction within the context of a replication slot.
    ///
    /// The transaction is started with a repeatable read isolation level and uses the
    /// snapshot associated with the provided slot.
    async fn new(client: PgReplicationClient) -> EtlResult<Self> {
        client.begin_tx().await?;

        Ok(Self { client })
    }

    /// Retrieves the schema information for the specified tables.
    ///
    /// If a publication is specified, only columns of the tables included in that publication
    /// will be returned.
    pub async fn get_table_schemas(
        &self,
        table_ids: &[TableId],
    ) -> EtlResult<HashMap<TableId, TableSchema>> {
        self.client.get_table_schemas(table_ids).await
    }

    /// Retrieves the schema information for the supplied table.
    ///
    /// If a publication is specified, only columns included in that publication
    /// will be returned.
    pub async fn get_table_schema(&self, table_id: TableId) -> EtlResult<TableSchema> {
        self.client.get_table_schema(table_id).await
    }

    /// Creates a COPY stream for reading data from the specified table.
    ///
    /// The stream will include only the columns specified in `column_schemas`.
    pub async fn get_table_copy_stream<'a>(
        &self,
        table_id: TableId,
        column_schemas: impl Iterator<Item = &'a ColumnSchema>,
        publication_name: Option<&str>,
    ) -> EtlResult<CopyOutStream> {
        self.client
            .get_table_copy_stream(table_id, column_schemas, publication_name)
            .await
    }

    /// Retrieves the names of columns being replicated for a table in a publication.
    ///
    /// Returns a HashSet containing the names of columns that are included in the publication
    /// for the specified table. If no publication is specified, returns all column names from
    /// the table schema.
    pub async fn get_replicated_column_names(
        &self,
        table_id: TableId,
        table_schema: &TableSchema,
        publication_name: Option<&str>,
    ) -> EtlResult<HashSet<String>> {
        self.client
            .get_replicated_column_names(table_id, table_schema, publication_name)
            .await
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

/// A client for interacting with Postgres's logical replication features.
///
/// This client provides methods for creating replication slots, managing transactions,
/// and streaming changes from the database.
#[derive(Debug, Clone)]
pub struct PgReplicationClient {
    client: Arc<Client>,
    server_version: Option<NonZeroI32>,
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
        let mut config: Config = pg_connection_config.clone().with_db();
        config.replication_mode(ReplicationMode::Logical);

        let (client, connection) = config.connect(NoTls).await?;

        let server_version = connection
            .parameter("server_version")
            .and_then(extract_server_version);

        spawn_postgres_connection::<NoTls>(connection);

        info!("successfully connected to postgres without tls");

        Ok(PgReplicationClient {
            client: Arc::new(client),
            server_version,
        })
    }

    /// Establishes a TLS-encrypted connection to Postgres.
    ///
    /// The connection is configured for logical replication mode
    async fn connect_tls(pg_connection_config: PgConnectionConfig) -> EtlResult<Self> {
        let mut config: Config = pg_connection_config.clone().with_db();
        config.replication_mode(ReplicationMode::Logical);

        let mut root_store = rustls::RootCertStore::empty();
        if pg_connection_config.tls.enabled {
            let mut root_certs_reader =
                BufReader::new(pg_connection_config.tls.trusted_root_certs.as_bytes());
            for cert in rustls_pemfile::certs(&mut root_certs_reader) {
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

        spawn_postgres_connection::<MakeRustlsConnect>(connection);

        info!("successfully connected to postgres with tls");

        Ok(PgReplicationClient {
            client: Arc::new(client),
            server_version,
        })
    }

    /// Checks if the underlying connection is closed.
    pub fn is_closed(&self) -> bool {
        self.client.is_closed()
    }

    /// Creates a new logical replication slot with the specified name and a transaction which is set
    /// on the snapshot exported by the slot creation.
    pub async fn create_slot_with_transaction(
        &self,
        slot_name: &str,
    ) -> EtlResult<(PgReplicationSlotTransaction, CreateSlotResult)> {
        // TODO: check if we want to consume the client and return it on commit to avoid any other
        //  operations on a connection that has started a transaction.
        let transaction = PgReplicationSlotTransaction::new(self.clone()).await?;
        let slot = self.create_slot_internal(slot_name, true).await?;

        Ok((transaction, slot))
    }

    /// Creates a new logical replication slot with the specified name and no snapshot.
    pub async fn create_slot(&self, slot_name: &str) -> EtlResult<CreateSlotResult> {
        self.create_slot_internal(slot_name, false).await
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
                info!("using existing replication slot '{}'", slot_name);

                Ok(GetOrCreateSlotResult::GetSlot(slot))
            }
            Err(err) if err.kind() == ErrorKind::ReplicationSlotNotFound => {
                info!("creating new replication slot '{}'", slot_name);

                let create_result = self.create_slot_internal(slot_name, false).await?;

                Ok(GetOrCreateSlotResult::CreateSlot(create_result))
            }
            Err(e) => Err(e),
        }
    }

    /// Deletes a replication slot with the specified name.
    ///
    /// Returns an error if the slot doesn't exist or if there are any issues with the deletion.
    pub async fn delete_slot(&self, slot_name: &str) -> EtlResult<()> {
        info!("deleting replication slot '{}'", slot_name);
        // Do not convert the query or the options to lowercase, see comment in `create_slot_internal`.
        let query = format!(
            r#"DROP_REPLICATION_SLOT {} WAIT;"#,
            quote_identifier(slot_name)
        );

        match self.client.simple_query(&query).await {
            Ok(_) => {
                info!("successfully deleted replication slot '{}'", slot_name);

                Ok(())
            }
            Err(err) => {
                if let Some(code) = err.code()
                    && *code == SqlState::UNDEFINED_OBJECT
                {
                    warn!(
                        "attempted to delete non-existent replication slot '{}'",
                        slot_name
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

                error!("failed to delete replication slot '{}': {}", slot_name, err);

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
        info!(
            "starting logical replication from publication '{}' with slot named '{}' at lsn {}",
            publication_name, slot_name, start_lsn
        );

        // Do not convert the query or the options to lowercase, see comment in `create_slot_internal`.
        let options = format!(
            r#"("proto_version" '1', "publication_names" {}, "messages" 'true')"#,
            quote_literal(quote_identifier(publication_name).as_ref())
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
    /// The `use_snapshot` parameter determines whether to use a snapshot for the slot creation.
    async fn create_slot_internal(
        &self,
        slot_name: &str,
        use_snapshot: bool,
    ) -> EtlResult<CreateSlotResult> {
        // Do not convert the query or the options to lowercase, since the lexer for
        // replication commands (repl_scanner.l) in Postgres code expects the commands
        // in uppercase. This probably should be fixed in upstream, but for now we will
        // keep the commands in uppercase.
        let snapshot_option = if use_snapshot {
            "USE_SNAPSHOT"
        } else {
            "NOEXPORT_SNAPSHOT"
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

    /// Retrieves schema information for multiple tables.
    ///
    /// Tables without primary keys will be skipped and logged with a warning.
    async fn get_table_schemas(
        &self,
        table_ids: &[TableId],
    ) -> EtlResult<HashMap<TableId, TableSchema>> {
        let mut table_schemas = HashMap::new();

        // TODO: consider if we want to fail when at least one table was missing or not.
        for table_id in table_ids {
            let table_schema = self.get_table_schema(*table_id).await?;

            // TODO: this warning and skipping should not happen in this method,
            //  but rather higher in the stack.
            if !table_schema.has_primary_keys() {
                warn!(
                    "table {} with id {} will not be copied because it has no primary key",
                    table_schema.name, table_schema.id
                );
                continue;
            }

            table_schemas.insert(table_schema.id, table_schema);
        }

        Ok(table_schemas)
    }

    /// Retrieves the schema for a single table.
    ///
    /// If a publication is specified, only columns included in that publication
    /// will be returned.
    async fn get_table_schema(&self, table_id: TableId) -> EtlResult<TableSchema> {
        let table_name = self.get_table_name(table_id).await?;
        let column_schemas = self.get_column_schemas(table_id).await?;

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

    /// Retrieves schema information for all columns in a table.
    ///
    /// All columns are initially loaded with `replicated = true`. The replication status
    /// will be updated later when relation messages are received during CDC streaming.
    async fn get_column_schemas(&self, table_id: TableId) -> EtlResult<Vec<ColumnSchema>> {
        let column_info_query = format!(
            r#"
            select
                name,
                type_oid,
                type_modifier,
                ordinal_position,
                primary_key_ordinal_position,
                nullable
            from etl.describe_table_schema({table_id})
            order by ordinal_position
            "#
        );

        let mut column_schemas = vec![];
        for message in self.client.simple_query(&column_info_query).await? {
            if let SimpleQueryMessage::Row(row) = message {
                let name = Self::get_row_value::<String>(&row, "name", "pg_attribute").await?;
                let type_oid = Self::get_row_value::<u32>(&row, "type_oid", "pg_type").await?;
                let type_modifier =
                    Self::get_row_value::<i32>(&row, "type_modifier", "pg_attribute").await?;
                let ordinal_position =
                    Self::get_row_value::<i32>(&row, "ordinal_position", "pg_attribute").await?;
                let primary_key_ordinal_position: Option<i32> = row
                    .try_get("primary_key_ordinal_position")?
                    .and_then(|s: &str| s.parse().ok());
                let nullable_str =
                    Self::get_row_value::<String>(&row, "nullable", "pg_attribute").await?;
                let nullable = nullable_str == "t" || nullable_str == "true";

                let typ = convert_type_oid_to_type(type_oid);

                column_schemas.push(ColumnSchema::new(
                    name,
                    typ,
                    type_modifier,
                    ordinal_position,
                    primary_key_ordinal_position,
                    nullable,
                ))
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

    /// Retrieves the names of columns being replicated for a table in a publication.
    ///
    /// Returns a HashSet containing the names of columns that are included in the publication
    /// for the specified table. If no publication is specified, returns all column names from
    /// the table schema. If the PostgreSQL version is below 15 (which doesn't support column
    /// filtering), returns all column names.
    ///
    /// This method should be called in the same transaction as describe_table_schema to ensure
    /// consistency during initial table sync.
    pub async fn get_replicated_column_names(
        &self,
        table_id: TableId,
        table_schema: &TableSchema,
        publication_name: Option<&str>,
    ) -> EtlResult<HashSet<String>> {
        // Column filtering in publications was added in Postgres 15. For earlier versions,
        // all columns are replicated.
        if below_version!(self.server_version, POSTGRES_15) {
            return Ok(table_schema
                .column_schemas
                .iter()
                .map(|cs| cs.name.clone())
                .collect());
        }

        // If no publication is specified, all columns are replicated
        let publication = match publication_name {
            Some(publication) => publication,
            None => {
                return Ok(table_schema
                    .column_schemas
                    .iter()
                    .map(|cs| cs.name.clone())
                    .collect());
            }
        };

        // Query pg_publication_tables to get the attnames array which contains the column names
        // being replicated. In PG15+, attnames is NULL when all columns are published.
        let column_query = format!(
            "select pt.attnames as column_names
             from pg_publication_tables pt
             join pg_namespace n on n.nspname = pt.schemaname
             join pg_class c on c.relnamespace = n.oid AND c.relname = pt.tablename
             where pt.pubname = {} and c.oid = {};",
            quote_literal(publication),
            table_id,
        );

        let results = self.client.simple_query(&column_query).await?;

        for result in results {
            if let SimpleQueryMessage::Row(row) = result {
                let column_names: Option<&str> = row.try_get("column_names")?;
                match column_names {
                    // NULL means all columns are published
                    None => {
                        return Ok(table_schema
                            .column_schemas
                            .iter()
                            .map(|cs| cs.name.clone())
                            .collect());
                    }
                    // Parse the array format: {col1,col2,col3}
                    Some(names_str) => {
                        let names: HashSet<String> = names_str
                            .trim_matches(|c| c == '{' || c == '}')
                            .split(',')
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                            .collect();
                        return Ok(names);
                    }
                }
            }
        }

        // If no row was found, the table is not in the publication.
        // Return all columns as a fallback (this shouldn't normally happen).
        Ok(table_schema
            .column_schemas
            .iter()
            .map(|cs| cs.name.clone())
            .collect())
    }

    /// Creates a COPY stream for reading data from a table using its OID.
    ///
    /// The stream will include only the specified columns and use text format, and respect
    /// publication row filters (if a publication is specified)
    pub async fn get_table_copy_stream<'a>(
        &self,
        table_id: TableId,
        column_schemas: impl Iterator<Item = &'a ColumnSchema>,
        publication: Option<&str>,
    ) -> EtlResult<CopyOutStream> {
        let column_list = column_schemas
            .map(|col| quote_identifier(&col.name))
            .collect::<Vec<_>>()
            .join(", ");

        let table_name = self.get_table_name(table_id).await?;
        let filter = self.get_row_filter(table_id, publication).await?;

        let copy_query = if let Some(pred) = filter {
            // Use select-form so we can add where.
            format!(
                r#"copy (select {} from {} where {}) to stdout with (format text);"#,
                column_list,
                table_name.as_quoted_identifier(),
                pred,
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
