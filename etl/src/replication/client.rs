use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::utils::tokio::MakeRustlsConnect;
use crate::{bail, etl_error};
use etl_config::shared::{IntoConnectOptions, PgConnectionConfig};
use etl_postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use etl_postgres::types::convert_type_oid_to_type;
use pg_escape::{quote_identifier, quote_literal};
use postgres_replication::LogicalReplicationStream;
use rustls::ClientConfig;
use std::collections::HashMap;
use std::fmt;
use std::io::BufReader;
use std::sync::Arc;
use tokio_postgres::error::SqlState;
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{
    Client, Config, Connection, CopyOutStream, NoTls, SimpleQueryMessage, SimpleQueryRow, Socket,
    config::ReplicationMode, types::PgLsn,
};
use tracing::{Instrument, error, info, warn};

/// Spawns a background task to monitor a PostgreSQL connection until it terminates.
///
/// The task will log when the connection terminates, either successfully or with an error.
fn spawn_postgres_connection<T>(connection: Connection<Socket, T::Stream>)
where
    T: MakeTlsConnect<Socket>,
    T::Stream: Send + 'static,
{
    // TODO: maybe return a handle for this task to keep track of it.
    let span = tracing::Span::current();
    let task = async move {
        if let Err(e) = connection.await {
            error!("an error occurred during the Postgres connection: {}", e);
            return;
        }

        info!("postgres connection terminated successfully")
    }
    .instrument(span);

    tokio::spawn(task);
}

#[derive(Debug, Clone)]
pub struct CreateSlotResult {
    pub consistent_point: PgLsn,
}

#[derive(Debug, Clone)]
pub struct GetSlotResult {
    pub confirmed_flush_lsn: PgLsn,
}

#[derive(Debug, Clone)]
pub enum GetOrCreateSlotResult {
    CreateSlot(CreateSlotResult),
    GetSlot(GetSlotResult),
}

impl GetOrCreateSlotResult {
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
        publication_name: Option<&str>,
    ) -> EtlResult<HashMap<TableId, TableSchema>> {
        self.client
            .get_table_schemas(table_ids, publication_name)
            .await
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
    ) -> EtlResult<CopyOutStream> {
        self.client
            .get_table_copy_stream(table_id, column_schemas)
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

/// A client for interacting with PostgreSQL's logical replication features.
///
/// This client provides methods for creating replication slots, managing transactions,
/// and streaming changes from the database.
#[derive(Debug, Clone)]
pub struct PgReplicationClient {
    client: Arc<Client>,
}

impl PgReplicationClient {
    /// Establishes a connection to PostgreSQL. The connection uses TLS if configured in the
    /// supplied [`PgConnectionConfig`].
    ///
    /// The connection is configured for logical replication mode
    pub async fn connect(pg_connection_config: PgConnectionConfig) -> EtlResult<Self> {
        match pg_connection_config.tls.enabled {
            true => PgReplicationClient::connect_tls(pg_connection_config).await,
            false => PgReplicationClient::connect_no_tls(pg_connection_config).await,
        }
    }

    /// Establishes a connection to PostgreSQL without TLS encryption.
    ///
    /// The connection is configured for logical replication mode.
    async fn connect_no_tls(pg_connection_config: PgConnectionConfig) -> EtlResult<Self> {
        let mut config: Config = pg_connection_config.clone().with_db();
        config.replication_mode(ReplicationMode::Logical);

        let (client, connection) = config.connect(NoTls).await?;
        spawn_postgres_connection::<NoTls>(connection);

        info!("successfully connected to postgres without tls");

        Ok(PgReplicationClient {
            client: Arc::new(client),
        })
    }

    /// Establishes a TLS-encrypted connection to PostgreSQL.
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
        spawn_postgres_connection::<MakeRustlsConnect>(connection);

        info!("successfully connected to postgres with tls");

        Ok(PgReplicationClient {
            client: Arc::new(client),
        })
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
    pub async fn get_publication_table_ids(
        &self,
        publication_name: &str,
    ) -> EtlResult<Vec<TableId>> {
        let publication_query = format!(
            "select c.oid from pg_publication_tables pt 
         join pg_class c on c.relname = pt.tablename 
         join pg_namespace n on n.oid = c.relnamespace AND n.nspname = pt.schemaname 
         where pt.pubname = {};",
            quote_literal(publication_name)
        );

        let mut table_ids = vec![];
        for msg in self.client.simple_query(&publication_query).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                // For the sake of simplicity, we refer to the table oid as table id.
                let table_id = Self::get_row_value::<TableId>(&row, "oid", "pg_class").await?;
                table_ids.push(table_id);
            }
        }

        Ok(table_ids)
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
            "Failed to create replication slot"
        ))
    }

    /// Retrieves schema information for multiple tables.
    ///
    /// Tables without primary keys will be skipped and logged with a warning.
    async fn get_table_schemas(
        &self,
        table_ids: &[TableId],
        publication_name: Option<&str>,
    ) -> EtlResult<HashMap<TableId, TableSchema>> {
        let mut table_schemas = HashMap::new();

        // TODO: consider if we want to fail when at least one table was missing or not.
        for table_id in table_ids {
            let table_schema = self.get_table_schema(*table_id, publication_name).await?;

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
            "Table not found",
            format!("Table not found in database (table id: {})", table_id)
        );
    }

    /// Retrieves schema information for all columns in a table.
    ///
    /// If a publication is specified, only columns included in that publication
    /// will be returned.
    async fn get_column_schemas(
        &self,
        table_id: TableId,
        publication: Option<&str>,
    ) -> EtlResult<Vec<ColumnSchema>> {
        let (pub_cte, pub_pred) = if let Some(publication) = publication {
            (
                format!(
                    "with pub_attrs as (
                        select unnest(r.prattrs)
                        from pg_publication_rel r
                        left join pg_publication p on r.prpubid = p.oid
                        where p.pubname = {publication}
                        and r.prrelid = {table_id}
                    )",
                    publication = quote_literal(publication),
                ),
                "and (
                    case (select count(*) from pub_attrs)
                    when 0 then true
                    else (a.attnum in (select * from pub_attrs))
                    end
                )",
            )
        } else {
            ("".into(), "")
        };

        let column_info_query = format!(
            "{pub_cte}
            select a.attname,
                a.atttypid,
                a.atttypmod,
                a.attnotnull,
                coalesce(i.indisprimary, false) as primary
            from pg_attribute a
            left join pg_index i
                on a.attrelid = i.indrelid
                and a.attnum = any(i.indkey)
                and i.indisprimary = true
            where a.attnum > 0::int2
            and not a.attisdropped
            and a.attgenerated = ''
            and a.attrelid = {table_id}
            {pub_pred}
            order by a.attnum
            ",
        );

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

    /// Creates a COPY stream for reading data from a table using its OID.
    ///
    /// The stream will include only the specified columns and use text format.
    pub async fn get_table_copy_stream(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
    ) -> EtlResult<CopyOutStream> {
        let column_list = column_schemas
            .iter()
            .map(|col| quote_identifier(&col.name))
            .collect::<Vec<_>>()
            .join(", ");

        let table_name = self.get_table_name(table_id).await?;

        // TODO: allow passing in format binary or text
        let copy_query = format!(
            r#"copy {} ({}) to stdout with (format text);"#,
            table_name.as_quoted_identifier(),
            column_list
        );

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
            "Column not found",
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
