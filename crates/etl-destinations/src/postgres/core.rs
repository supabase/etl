//! Postgres destination optimized for write throughput.

use std::{
    collections::{HashMap, HashSet},
    error::Error as StdError,
    sync::Arc,
};

use bytes::BytesMut;
use etl::{
    destination::{
        Destination,
        async_result::{TruncateTableResult, WriteEventsResult, WriteTableRowsResult},
    },
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    types::{
        ArrayCell, Cell, Event, ReplicatedTableSchema, TableId, TableRow, UpdatedTableRow,
        is_array_type,
    },
};
use pg_escape::quote_identifier;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_postgres::{
    Client, Config, NoTls,
    types::{IsNull, ToSql, Type},
};
use tracing::{debug, warn};

/// Configuration for a [`PostgresDestination`].
#[derive(Debug, Clone)]
pub struct PostgresDestinationConfig {
    /// Destination schema where tables are created.
    schema: String,
    /// Maximum number of active destination connections.
    pool_size: usize,
    /// Maximum rows per multi-row `insert` statement.
    max_insert_rows: usize,
    /// Maximum bind parameters per multi-row `insert` statement.
    max_insert_params: usize,
    /// Whether each destination session sets `synchronous_commit = off`.
    synchronous_commit_off: bool,
    /// Whether destination tables are created as `unlogged`.
    unlogged_tables: bool,
}

impl PostgresDestinationConfig {
    /// Practical upper bound below the signed 16-bit protocol count used here.
    pub const DEFAULT_MAX_INSERT_PARAMS: usize = 30_000;
    /// Default maximum number of rows in one generated `insert` statement.
    pub const DEFAULT_MAX_INSERT_ROWS: usize = 2_000;
    /// Default destination connection pool size.
    pub const DEFAULT_POOL_SIZE: usize = 16;
    /// Default destination schema.
    pub const DEFAULT_SCHEMA: &'static str = "etl_destination";

    /// Creates a config with default throughput-oriented options.
    pub fn new(schema: impl Into<String>) -> Self {
        Self { schema: schema.into(), ..Self::default() }
    }

    /// Sets the maximum number of active destination connections.
    pub fn with_pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = pool_size;
        self
    }

    /// Sets the maximum rows per generated multi-row `insert`.
    pub fn with_max_insert_rows(mut self, max_insert_rows: usize) -> Self {
        self.max_insert_rows = max_insert_rows;
        self
    }

    /// Sets the maximum bind parameters per generated multi-row `insert`.
    pub fn with_max_insert_params(mut self, max_insert_params: usize) -> Self {
        self.max_insert_params = max_insert_params;
        self
    }

    /// Controls whether destination sessions use durable synchronous commits.
    pub fn with_synchronous_commit(mut self, synchronous_commit: bool) -> Self {
        self.synchronous_commit_off = !synchronous_commit;
        self
    }

    /// Controls whether destination tables are created as `unlogged`.
    pub fn with_unlogged_tables(mut self, unlogged_tables: bool) -> Self {
        self.unlogged_tables = unlogged_tables;
        self
    }

    /// Validates the config before opening destination connections.
    fn validate(&self) -> EtlResult<()> {
        if self.schema.trim().is_empty() {
            return Err(etl_error!(
                ErrorKind::DestinationError,
                "Postgres destination schema cannot be empty"
            ));
        }

        if self.pool_size == 0 {
            return Err(etl_error!(
                ErrorKind::DestinationError,
                "Postgres destination pool size must be greater than zero"
            ));
        }

        if self.max_insert_rows == 0 {
            return Err(etl_error!(
                ErrorKind::DestinationError,
                "Postgres destination max insert rows must be greater than zero"
            ));
        }

        if self.max_insert_params == 0 || self.max_insert_params > i16::MAX as usize {
            return Err(etl_error!(
                ErrorKind::DestinationError,
                "Postgres destination max insert params must be between 1 and 32767"
            ));
        }

        Ok(())
    }
}

impl Default for PostgresDestinationConfig {
    fn default() -> Self {
        Self {
            schema: Self::DEFAULT_SCHEMA.to_owned(),
            pool_size: Self::DEFAULT_POOL_SIZE,
            max_insert_rows: Self::DEFAULT_MAX_INSERT_ROWS,
            max_insert_params: Self::DEFAULT_MAX_INSERT_PARAMS,
            synchronous_commit_off: true,
            unlogged_tables: false,
        }
    }
}

/// Postgres destination using pooled multi-row `insert` writes.
#[derive(Clone)]
pub struct PostgresDestination {
    /// Shared destination state.
    inner: Arc<PostgresDestinationInner>,
}

/// Shared state for [`PostgresDestination`].
struct PostgresDestinationInner {
    /// Tokio Postgres connection config.
    config: Config,
    /// Destination behavior options.
    config_options: PostgresDestinationConfig,
    /// Semaphore limiting active destination connections.
    permits: Arc<Semaphore>,
    /// Idle clients available for reuse.
    idle_clients: Mutex<Vec<Client>>,
    /// Table IDs whose target table has already been prepared.
    prepared_tables: Mutex<HashSet<TableId>>,
}

/// A checked-out destination client and its active connection permit.
struct PooledClient {
    /// Checked-out tokio-postgres client.
    client: Client,
    /// Permit held while the client is active.
    _permit: OwnedSemaphorePermit,
}

/// Pending streaming rows grouped by destination table.
struct PendingRows {
    /// Runtime schema for the grouped rows.
    schema: ReplicatedTableSchema,
    /// Rows to append for the schema.
    rows: Vec<TableRow>,
}

impl PendingRows {
    /// Creates a pending row group for a table schema.
    fn new(schema: ReplicatedTableSchema) -> Self {
        Self { schema, rows: Vec::new() }
    }
}

impl PostgresDestination {
    /// Creates a new Postgres destination.
    ///
    /// This constructor currently connects with [`NoTls`]. Callers that need
    /// TLS should use a TLS-capable constructor once one is added.
    pub async fn new(
        connection_config: Config,
        destination_config: PostgresDestinationConfig,
    ) -> EtlResult<Self> {
        destination_config.validate()?;
        let destination = Self {
            inner: Arc::new(PostgresDestinationInner {
                config: connection_config,
                permits: Arc::new(Semaphore::new(destination_config.pool_size)),
                idle_clients: Mutex::new(Vec::with_capacity(destination_config.pool_size)),
                prepared_tables: Mutex::new(HashSet::new()),
                config_options: destination_config,
            }),
        };
        destination.prepare_schema().await?;

        Ok(destination)
    }

    /// Ensures the destination schema exists.
    async fn prepare_schema(&self) -> EtlResult<()> {
        let pooled = self.acquire_client().await?;
        let schema = quote_identifier(&self.inner.config_options.schema);
        let result =
            pooled.client.batch_execute(&format!("create schema if not exists {schema}")).await;
        self.release_client(pooled, result.is_ok()).await;

        result.map_err(|source| {
            postgres_query_error("Postgres destination schema creation failed", source)
        })
    }

    /// Acquires a client from the pool, opening a new connection when needed.
    async fn acquire_client(&self) -> EtlResult<PooledClient> {
        let permit = self.inner.permits.clone().acquire_owned().await.map_err(|error| {
            etl_error!(
                ErrorKind::DestinationError,
                "Postgres destination connection pool is closed",
                source: error
            )
        })?;

        let client = loop {
            let maybe_client = self.inner.idle_clients.lock().await.pop();
            let Some(client) = maybe_client else {
                break self.connect_client().await?;
            };
            if !client.is_closed() {
                break client;
            }
        };

        Ok(PooledClient { client, _permit: permit })
    }

    /// Returns a checked-out client to the pool when it is still usable.
    async fn release_client(&self, pooled: PooledClient, reusable: bool) {
        if !reusable || pooled.client.is_closed() {
            return;
        }

        let mut clients = self.inner.idle_clients.lock().await;
        if clients.len() < self.inner.config_options.pool_size {
            clients.push(pooled.client);
        }
    }

    /// Opens and configures a new destination connection.
    async fn connect_client(&self) -> EtlResult<Client> {
        let (client, connection) = self.inner.config.connect(NoTls).await.map_err(|source| {
            etl_error!(
                ErrorKind::DestinationConnectionFailed,
                "Postgres destination connection failed",
                source: source
            )
        })?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                warn!(error = %error, "postgres destination connection closed");
            }
        });

        let mut setup =
            String::from("set client_min_messages = warning; set statement_timeout = 0;");
        if self.inner.config_options.synchronous_commit_off {
            setup.push_str(" set synchronous_commit = off;");
        }
        client.batch_execute(&setup).await.map_err(|source| {
            postgres_query_error("Postgres destination session setup failed", source)
        })?;

        Ok(client)
    }

    /// Ensures a target table exists for the replicated table schema.
    async fn ensure_table(&self, replicated_table_schema: &ReplicatedTableSchema) -> EtlResult<()> {
        let table_id = replicated_table_schema.id();
        if self.inner.prepared_tables.lock().await.contains(&table_id) {
            return Ok(());
        }

        let pooled = self.acquire_client().await?;
        let result = self.ensure_table_with_client(&pooled.client, replicated_table_schema).await;
        self.release_client(pooled, result.is_ok()).await;
        result?;

        self.inner.prepared_tables.lock().await.insert(table_id);
        Ok(())
    }

    /// Creates the target table using throughput-oriented destination DDL.
    async fn ensure_table_with_client(
        &self,
        client: &Client,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let target_table = self.target_table(replicated_table_schema.id());
        let columns = replicated_table_schema
            .column_schemas()
            .map(|column| {
                format!("{} {}", quote_identifier(&column.name), postgres_type_sql(&column.typ))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let persistence = if self.inner.config_options.unlogged_tables { "unlogged " } else { "" };
        let sql = format!(
            "create {persistence}table if not exists {} ({columns})",
            target_table.qualified_name
        );

        debug!(
            table_id = %replicated_table_schema.id(),
            target_table = %target_table.qualified_name,
            "ensuring postgres destination table"
        );
        client.batch_execute(&sql).await.map_err(|source| {
            postgres_query_error("Postgres destination table creation failed", source)
        })?;

        Ok(())
    }

    /// Returns the target table for a replicated source table ID.
    fn target_table(&self, table_id: TableId) -> TargetTable {
        TargetTable {
            qualified_name: format!(
                "{}.{}",
                quote_identifier(&self.inner.config_options.schema),
                quote_identifier(&format!("table_{}", table_id.into_inner()))
            ),
        }
    }

    /// Writes table rows using multi-row parameterized `insert` statements.
    async fn write_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        self.ensure_table(replicated_table_schema).await?;
        if table_rows.is_empty() {
            return Ok(());
        }

        let pooled = self.acquire_client().await?;
        let result =
            self.write_rows_with_client(&pooled.client, replicated_table_schema, table_rows).await;
        self.release_client(pooled, result.is_ok()).await;

        result
    }

    /// Writes all rows for one destination call through a single connection.
    async fn write_rows_with_client(
        &self,
        client: &Client,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let target_table = self.target_table(replicated_table_schema.id());
        let columns = replicated_table_schema.column_schemas().collect::<Vec<_>>();
        let column_count = columns.len();
        if column_count == 0 {
            return Ok(());
        }

        if column_count > self.inner.config_options.max_insert_params {
            return Err(etl_error!(
                ErrorKind::DestinationError,
                "Postgres destination table has more columns than the insert parameter limit",
                format!(
                    "Table {} has {} replicated columns but max insert params is {}",
                    replicated_table_schema.name(),
                    column_count,
                    self.inner.config_options.max_insert_params
                )
            ));
        }

        let rows_per_statement = self
            .inner
            .config_options
            .max_insert_rows
            .min(self.inner.config_options.max_insert_params / column_count)
            .max(1);
        let column_names = columns
            .iter()
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let mut row_iter = table_rows.into_iter();

        loop {
            let mut values = Vec::with_capacity(rows_per_statement.saturating_mul(column_count));
            let mut rows_in_statement = 0usize;

            while rows_in_statement < rows_per_statement {
                let Some(row) = row_iter.next() else {
                    break;
                };
                let row_values = row.into_values();
                if row_values.len() != column_count {
                    return Err(etl_error!(
                        ErrorKind::DestinationError,
                        "Postgres destination row column count does not match schema",
                        format!(
                            "Table {} expected {} columns but row had {} values",
                            replicated_table_schema.name(),
                            column_count,
                            row_values.len()
                        )
                    ));
                }

                values.extend(row_values.into_iter().map(PostgresCell));
                rows_in_statement += 1;
            }

            if rows_in_statement == 0 {
                break;
            }

            let sql =
                build_insert_sql(&target_table, &column_names, column_count, rows_in_statement);
            let params =
                values.iter().map(|value| value as &(dyn ToSql + Sync)).collect::<Vec<_>>();
            client.execute(sql.as_str(), params.as_slice()).await.map_err(|source| {
                postgres_query_error("Postgres destination insert failed", source)
            })?;
        }

        Ok(())
    }

    /// Writes streaming events with append-only row handling.
    async fn write_events_inner(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut pending_rows = HashMap::<TableId, PendingRows>::new();

        for event in events {
            match event {
                Event::Insert(insert) => {
                    let table_id = insert.replicated_table_schema.id();
                    let entry = pending_rows.entry(table_id).or_insert_with(|| {
                        PendingRows::new(insert.replicated_table_schema.clone())
                    });
                    entry.rows.push(insert.table_row);
                }
                Event::Update(update) => {
                    let table_row = match update.updated_table_row {
                        UpdatedTableRow::Full(row) => row,
                        UpdatedTableRow::Partial(_) => continue,
                    };
                    let table_id = update.replicated_table_schema.id();
                    let entry = pending_rows.entry(table_id).or_insert_with(|| {
                        PendingRows::new(update.replicated_table_schema.clone())
                    });
                    entry.rows.push(table_row);
                }
                Event::Truncate(truncate) => {
                    self.flush_pending_rows(&mut pending_rows).await?;
                    for schema in truncate.truncated_tables {
                        self.truncate_rows(&schema).await?;
                    }
                }
                Event::Relation(relation) => {
                    self.flush_pending_rows(&mut pending_rows).await?;
                    self.ensure_table(&relation.replicated_table_schema).await?;
                }
                Event::Begin(_) | Event::Commit(_) | Event::Delete(_) | Event::Unsupported => {}
            }
        }

        self.flush_pending_rows(&mut pending_rows).await
    }

    /// Flushes grouped streaming rows to the destination.
    async fn flush_pending_rows(
        &self,
        pending_rows: &mut HashMap<TableId, PendingRows>,
    ) -> EtlResult<()> {
        let batches = std::mem::take(pending_rows);
        for (_, batch) in batches {
            self.write_rows(&batch.schema, batch.rows).await?;
        }

        Ok(())
    }

    /// Truncates the target table for a replicated source table.
    async fn truncate_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        self.ensure_table(replicated_table_schema).await?;
        let target_table = self.target_table(replicated_table_schema.id());
        let pooled = self.acquire_client().await?;
        let result = pooled
            .client
            .batch_execute(&format!("truncate table {}", target_table.qualified_name))
            .await;
        self.release_client(pooled, result.is_ok()).await;

        result
            .map_err(|source| postgres_query_error("Postgres destination truncate failed", source))
    }
}

impl Destination for PostgresDestination {
    fn name() -> &'static str {
        "postgres"
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.inner.idle_clients.lock().await.clear();
        Ok(())
    }

    async fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        let result = self.truncate_rows(replicated_table_schema).await;
        async_result.send(result);

        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let result = self.write_rows(replicated_table_schema, table_rows).await;
        async_result.send(result);

        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        let result = self.write_events_inner(events).await;
        async_result.send(result);

        Ok(())
    }
}

/// Target table name for a replicated table.
struct TargetTable {
    /// Fully quoted destination table name.
    qualified_name: String,
}

/// A local wrapper that lets cells bind as Postgres query params.
#[derive(Debug)]
struct PostgresCell(Cell);

impl ToSql for PostgresCell {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send>> {
        match &self.0 {
            Cell::Null => Ok(IsNull::Yes),
            Cell::Bool(value) => value.to_sql(ty, out),
            Cell::String(value) => value.to_sql(ty, out),
            Cell::I16(value) => value.to_sql(ty, out),
            Cell::I32(value) => value.to_sql(ty, out),
            Cell::U32(value) => value.to_sql(ty, out),
            Cell::I64(value) => value.to_sql(ty, out),
            Cell::F32(value) => value.to_sql(ty, out),
            Cell::F64(value) => value.to_sql(ty, out),
            Cell::Numeric(value) => value.to_sql(ty, out),
            Cell::Date(value) => value.to_sql(ty, out),
            Cell::Time(value) => value.to_sql(ty, out),
            Cell::Timestamp(value) => value.to_sql(ty, out),
            Cell::TimestampTz(value) => value.to_sql(ty, out),
            Cell::Uuid(value) => value.to_sql(ty, out),
            Cell::Json(value) => value.to_sql(ty, out),
            Cell::Bytes(value) => value.to_sql(ty, out),
            Cell::Array(value) => array_to_sql(value, ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send>> {
        self.to_sql(ty, out)
    }
}

/// Converts a cell array value into Postgres binary parameter format.
fn array_to_sql(
    value: &ArrayCell,
    ty: &Type,
    out: &mut BytesMut,
) -> Result<IsNull, Box<dyn StdError + Sync + Send>> {
    match value {
        ArrayCell::Bool(value) => value.to_sql(ty, out),
        ArrayCell::String(value) => value.to_sql(ty, out),
        ArrayCell::I16(value) => value.to_sql(ty, out),
        ArrayCell::I32(value) => value.to_sql(ty, out),
        ArrayCell::U32(value) => value.to_sql(ty, out),
        ArrayCell::I64(value) => value.to_sql(ty, out),
        ArrayCell::F32(value) => value.to_sql(ty, out),
        ArrayCell::F64(value) => value.to_sql(ty, out),
        ArrayCell::Numeric(value) => value.to_sql(ty, out),
        ArrayCell::Date(value) => value.to_sql(ty, out),
        ArrayCell::Time(value) => value.to_sql(ty, out),
        ArrayCell::Timestamp(value) => value.to_sql(ty, out),
        ArrayCell::TimestampTz(value) => value.to_sql(ty, out),
        ArrayCell::Uuid(value) => value.to_sql(ty, out),
        ArrayCell::Json(value) => value.to_sql(ty, out),
        ArrayCell::Bytes(value) => value.to_sql(ty, out),
    }
}

/// Builds a destination-scoped query error while preserving the source chain.
fn postgres_query_error(description: &'static str, source: tokio_postgres::Error) -> EtlError {
    etl_error!(ErrorKind::DestinationQueryFailed, description, source: source)
}

/// Builds a multi-row parameterized `insert` statement.
fn build_insert_sql(
    target_table: &TargetTable,
    column_names: &str,
    column_count: usize,
    rows: usize,
) -> String {
    let mut sql = String::with_capacity(rows.saturating_mul(column_count).saturating_mul(5));
    sql.push_str("insert into ");
    sql.push_str(&target_table.qualified_name);
    sql.push_str(" (");
    sql.push_str(column_names);
    sql.push_str(") values ");

    let mut param_idx = 1usize;
    for row_idx in 0..rows {
        if row_idx > 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        for column_idx in 0..column_count {
            if column_idx > 0 {
                sql.push_str(", ");
            }
            sql.push('$');
            sql.push_str(&param_idx.to_string());
            param_idx += 1;
        }
        sql.push(')');
    }

    sql
}

/// Maps source Postgres types into destination Postgres DDL types.
fn postgres_type_sql(typ: &Type) -> &'static str {
    match *typ {
        Type::BOOL => "boolean",
        Type::BOOL_ARRAY => "boolean[]",
        Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT | Type::MONEY => "text",
        Type::CHAR_ARRAY
        | Type::BPCHAR_ARRAY
        | Type::VARCHAR_ARRAY
        | Type::NAME_ARRAY
        | Type::TEXT_ARRAY
        | Type::MONEY_ARRAY => "text[]",
        Type::INT2 => "smallint",
        Type::INT2_ARRAY => "smallint[]",
        Type::INT4 => "integer",
        Type::INT4_ARRAY => "integer[]",
        Type::OID => "oid",
        Type::OID_ARRAY => "oid[]",
        Type::INT8 => "bigint",
        Type::INT8_ARRAY => "bigint[]",
        Type::FLOAT4 => "real",
        Type::FLOAT4_ARRAY => "real[]",
        Type::FLOAT8 => "double precision",
        Type::FLOAT8_ARRAY => "double precision[]",
        Type::NUMERIC => "numeric",
        Type::NUMERIC_ARRAY => "numeric[]",
        Type::BYTEA => "bytea",
        Type::BYTEA_ARRAY => "bytea[]",
        Type::DATE => "date",
        Type::DATE_ARRAY => "date[]",
        Type::TIME => "time",
        Type::TIME_ARRAY => "time[]",
        Type::TIMESTAMP => "timestamp",
        Type::TIMESTAMP_ARRAY => "timestamp[]",
        Type::TIMESTAMPTZ => "timestamptz",
        Type::TIMESTAMPTZ_ARRAY => "timestamptz[]",
        Type::UUID => "uuid",
        Type::UUID_ARRAY => "uuid[]",
        Type::JSON => "json",
        Type::JSON_ARRAY => "json[]",
        Type::JSONB => "jsonb",
        Type::JSONB_ARRAY => "jsonb[]",
        _ if is_array_type(typ) => "text[]",
        _ => "text",
    }
}
