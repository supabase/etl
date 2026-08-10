use std::{collections::HashMap, sync::Arc};

use etl::{
    pipeline::PipelineId,
    schema::{
        ColumnAlterationKind, ColumnPresenceChangeReason, ColumnSchema, SchemaOperation,
        SchemaPlan, TableId,
    },
};
use metrics::{counter, gauge, histogram};
use tokio::{
    sync::{Mutex, RwLock},
    time::{Instant, sleep},
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::snowflake::{
    Config, Error, Result, SnowpipeError,
    auth::{AuthManager, HttpExchanger, TokenProvider},
    config::{HTTP_CONNECT_TIMEOUT, HTTP_REQUEST_TIMEOUT},
    metrics::{
        ETL_SNOWFLAKE_STREAMING_DURABILITY_WAIT_FAILURES_TOTAL,
        ETL_SNOWFLAKE_STREAMING_DURABILITY_WAIT_SECONDS, ETL_SNOWFLAKE_STREAMING_PENDING_BYTES,
        ETL_SNOWFLAKE_STREAMING_PENDING_CHANNELS, ETL_SNOWFLAKE_STREAMING_PENDING_ROW_BATCHES,
    },
    schema,
    sql::{quote_identifier, quote_string_literal},
    sql_client::SqlClient,
    streaming::{
        AcceptedRowBatch, ChannelHandle, DEFAULT_COMMIT_POLL_INTERVAL, DEFAULT_COMMIT_WAIT_TIMEOUT,
        OffsetToken, PendingDurabilityTarget, RestStreamClient, RowBatch, StreamClient,
    },
};

type ChannelMap<C> = Arc<RwLock<HashMap<TableId, Arc<Mutex<ChannelHandle<C>>>>>>;

/// Process-local state carried between the two phases of a table-copy reset.
///
/// With event admission blocked, [`Client::detach_table_for_copy`] runs inside
/// the reset-preparation timeout: it discards the table's pending streaming
/// target, removes its cached channel, and carries any removed channel together
/// with its table name in this value.
///
/// [`Client::drop_detached_table_for_copy`] consumes the value outside that
/// timeout to drop the Snowpipe channel and Snowflake table. This split keeps
/// local draining and lock acquisition bounded without cancelling a remote
/// drop whose outcome would then be unknown.
///
/// This value does not block event admission. The caller must retain its
/// [`etl::destination::TaskSetDrainGuard`] until the remote drop returns.
pub(super) struct DetachedTableForCopy<C> {
    /// Previously cached channel handle, when one existed in this process.
    channel: Option<Arc<Mutex<ChannelHandle<C>>>>,
    /// Snowflake table name to drop after retiring local state.
    table_name: String,
}

/// Maximum accepted Snowflake row batches before forcing a durability wait.
const STREAMING_PENDING_MAX_ROW_BATCHES: usize = 64;

/// Maximum accepted compressed bytes before forcing a durability wait.
const STREAMING_PENDING_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Namespace for deterministic Snowflake truncate-attempt request IDs.
///
/// The namespace and encoded identity are a durable protocol: changing either
/// would prevent a restarted process from recovering an outcome-unknown SQL
/// request under its original ID.
const TRUNCATE_ATTEMPT_NAMESPACE: Uuid = Uuid::from_u128(0x4b929faf_9687_4544_a34f_f934369a37f7);

/// Version tag for the truncate-attempt identity encoding.
const TRUNCATE_ATTEMPT_IDENTITY_VERSION: &[u8] = b"supabase-etl/snowflake/truncate-attempt/v1";

/// Stable identity for one physical Snowflake truncate attempt.
#[derive(Clone, Copy)]
struct TruncateAttemptIdentity<'a> {
    /// ETL pipeline that owns the target channel.
    pipeline_id: PipelineId,
    /// Source Postgres table identifier.
    table_id: TableId,
    /// Exact Snowflake database identifier used by SQL.
    database: &'a str,
    /// Exact Snowflake schema identifier used by SQL.
    schema: &'a str,
    /// Exact Snowflake table identifier used by SQL.
    table: &'a str,
    /// Source truncate boundary assigned to the channel before SQL.
    truncate_offset: &'a OffsetToken,
    /// Snowflake channel creation timestamp observed before SQL.
    channel_created_on_ms: u64,
    /// Cumulative inserted rows observed before SQL.
    rows_inserted: u64,
}

impl TruncateAttemptIdentity<'_> {
    /// Derives the Snowflake SQL request ID for this physical attempt.
    fn request_id(self) -> Uuid {
        fn append_identity_component(identity: &mut Vec<u8>, component: &[u8]) {
            let component_len = u64::try_from(component.len())
                .expect("identity component length should fit into u64");
            identity.extend_from_slice(&component_len.to_be_bytes());
            identity.extend_from_slice(component);
        }

        let mut identity = Vec::new();
        append_identity_component(&mut identity, TRUNCATE_ATTEMPT_IDENTITY_VERSION);
        identity.extend_from_slice(&self.pipeline_id.to_be_bytes());
        identity.extend_from_slice(&self.table_id.into_inner().to_be_bytes());
        append_identity_component(&mut identity, self.database.as_bytes());
        append_identity_component(&mut identity, self.schema.as_bytes());
        append_identity_component(&mut identity, self.table.as_bytes());
        append_identity_component(&mut identity, self.truncate_offset.as_ref().as_bytes());
        identity.extend_from_slice(&self.channel_created_on_ms.to_be_bytes());
        identity.extend_from_slice(&self.rows_inserted.to_be_bytes());

        Uuid::new_v5(&TRUNCATE_ATTEMPT_NAMESPACE, &identity)
    }
}

/// Global accepted-but-not-durable Snowflake streaming state.
#[derive(Debug, Default)]
struct PendingDurabilityState {
    /// Pending durability targets keyed by table/channel id.
    targets: HashMap<TableId, PendingDurabilityTarget>,
    /// Total accepted row batches across pending targets.
    row_batches: usize,
    /// Total accepted compressed bytes across pending targets.
    bytes: usize,
}

impl PendingDurabilityState {
    /// Records a row batch accepted by Snowflake.
    fn record(&mut self, table_id: TableId, batch: AcceptedRowBatch) -> Result<()> {
        let row_batches = self.row_batches.checked_add(1).ok_or_else(|| {
            Error::Channel("Snowflake pending streaming batch count overflowed.".into())
        })?;
        let bytes = self.bytes.checked_add(batch.bytes).ok_or_else(|| {
            Error::Channel("Snowflake pending streaming byte count overflowed.".into())
        })?;

        match self.targets.get_mut(&table_id) {
            Some(target) => target.record(batch)?,
            None => {
                self.targets.insert(table_id, PendingDurabilityTarget::new(batch));
            }
        }

        self.row_batches = row_batches;
        self.bytes = bytes;
        Ok(())
    }

    /// Returns whether the pending window has reached its configured limit.
    fn limits_reached(&self) -> bool {
        self.row_batches >= STREAMING_PENDING_MAX_ROW_BATCHES
            || self.bytes >= STREAMING_PENDING_MAX_BYTES
    }

    /// Returns whether adding one row batch would exceed the pending window.
    fn would_exceed_limits(&self, batch_bytes: usize) -> bool {
        if self.is_empty() {
            return false;
        }

        self.limits_reached()
            || self.row_batches.saturating_add(1) > STREAMING_PENDING_MAX_ROW_BATCHES
            || self.bytes.saturating_add(batch_bytes) > STREAMING_PENDING_MAX_BYTES
    }

    /// Returns whether there is no accepted-but-not-durable streaming work.
    fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    /// Returns whether one table still has accepted-but-not-durable work.
    fn has_target(&self, table_id: TableId) -> bool {
        self.targets.contains_key(&table_id)
    }

    /// Clones the current pending targets for status polling.
    fn targets(&self) -> Vec<(TableId, PendingDurabilityTarget)> {
        self.targets.iter().map(|(table_id, target)| (*table_id, target.clone())).collect()
    }

    /// Discards one table's pending target and updates aggregate accounting.
    fn discard(&mut self, table_id: TableId) -> Result<()> {
        let Some(target) = self.targets.get(&table_id) else {
            return Ok(());
        };
        let target_row_batches = target.row_batches;
        let target_bytes = target.bytes;

        let row_batches = self.row_batches.checked_sub(target_row_batches).ok_or_else(|| {
            Error::Channel("Snowflake pending streaming batch accounting underflowed.".into())
        })?;
        let bytes = self.bytes.checked_sub(target_bytes).ok_or_else(|| {
            Error::Channel("Snowflake pending streaming byte accounting underflowed.".into())
        })?;

        self.targets.remove(&table_id);
        self.row_batches = row_batches;
        self.bytes = bytes;

        Ok(())
    }

    /// Removes targets whose current offset is covered by the proven target.
    fn clear_committed(&mut self, committed: &[(TableId, OffsetToken)]) -> Result<()> {
        for (table_id, committed_target) in committed {
            let should_clear = self
                .targets
                .get(table_id)
                .is_some_and(|target| &target.target_offset <= committed_target);

            if should_clear {
                self.discard(*table_id)?;
            }
        }

        Ok(())
    }

    /// Records pending-window gauges.
    fn observe_metrics(&self) {
        gauge!(ETL_SNOWFLAKE_STREAMING_PENDING_BYTES).set(self.bytes as f64);
        gauge!(ETL_SNOWFLAKE_STREAMING_PENDING_ROW_BATCHES).set(self.row_batches as f64);
        gauge!(ETL_SNOWFLAKE_STREAMING_PENDING_CHANNELS).set(self.targets.len() as f64);
    }
}

/// Snowflake API client.
///
/// Unifies the SQL REST API (DDL) and the Snowpipe Streaming API (channel
/// lifecycle and row ingestion).
pub struct Client<T, C = RestStreamClient<T>> {
    sql_client: Arc<SqlClient<T>>,
    stream_client: Arc<C>,
    database: String,
    schema: String,
    pipeline_id: PipelineId,
    channels: ChannelMap<C>,
    pending_durability: Arc<Mutex<PendingDurabilityState>>,
}

impl<T: TokenProvider, C: StreamClient> Clone for Client<T, C> {
    fn clone(&self) -> Self {
        Self {
            sql_client: Arc::clone(&self.sql_client),
            stream_client: Arc::clone(&self.stream_client),
            database: self.database.clone(),
            schema: self.schema.clone(),
            pipeline_id: self.pipeline_id,
            channels: Arc::clone(&self.channels),
            pending_durability: Arc::clone(&self.pending_durability),
        }
    }
}

/// Convenience constructor for the default client stack.
impl Client<AuthManager<HttpExchanger>> {
    pub fn new(auth: Arc<AuthManager<HttpExchanger>>, pipeline_id: PipelineId) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(HTTP_CONNECT_TIMEOUT)
            .timeout(HTTP_REQUEST_TIMEOUT)
            .build()
            .expect("failed to build HTTP client");
        let config = auth.config().clone_without_credentials();
        let database = config.database.clone();
        let schema = config.schema.clone();
        let stream_client = Arc::new(RestStreamClient::new(
            config.account_url().to_owned(),
            Arc::clone(&auth),
            http.clone(),
        ));
        let sql_client = SqlClient::new(config, auth, http);
        Self::with_clients(sql_client, stream_client, database, schema, pipeline_id)
    }

    /// Verify Snowflake connectivity.
    ///
    /// Check that credentials are valid and the target database and schema
    /// exist.
    pub async fn validate_connectivity(config: Config) -> Result<()> {
        let auth = Arc::new(AuthManager::new(config)?);

        let http = reqwest::Client::builder()
            .connect_timeout(HTTP_CONNECT_TIMEOUT)
            .timeout(HTTP_REQUEST_TIMEOUT)
            .build()
            .map_err(Error::HttpTransport)?;

        let config = auth.config().clone_without_credentials();
        let database = config.database.clone();
        let schema = config.schema.clone();
        let sql = SqlClient::new(config, auth, http);

        // `SHOW DATABASES` runs on Cloud Services (no warehouse needed).
        let db_pattern = quote_string_literal(&database);
        let resp = sql.execute_statement(&format!("SHOW DATABASES LIKE {db_pattern}")).await?;
        let db_exists = resp.data.is_some_and(|rows| {
            rows.iter().any(|row| row.get(1).and_then(serde_json::Value::as_str) == Some(&database))
        });
        if !db_exists {
            return Err(Error::DatabaseNotFound(database));
        }

        // `SHOW SCHEMAS` also runs on Cloud Services.
        let db_ident = quote_identifier(&database);
        let schema_pattern = quote_string_literal(&schema);
        let resp = sql
            .execute_statement(&format!(
                "SHOW SCHEMAS LIKE {schema_pattern} IN DATABASE {db_ident}"
            ))
            .await?;
        let schema_exists = resp.data.is_some_and(|rows| {
            rows.iter().any(|row| row.get(1).and_then(serde_json::Value::as_str) == Some(&schema))
        });
        if !schema_exists {
            return Err(Error::SchemaNotFound { database, schema });
        }

        let resp = sql
            .execute_statement("SHOW PARAMETERS LIKE 'QUOTED_IDENTIFIERS_IGNORE_CASE' IN SESSION")
            .await?;
        let ignore_quoted_identifier_case = resp.data.as_deref().and_then(|rows| {
            rows.iter().find_map(|row| {
                let name = row.first()?.as_str()?;
                name.eq_ignore_ascii_case("QUOTED_IDENTIFIERS_IGNORE_CASE")
                    .then(|| row.get(1)?.as_str())
                    .flatten()
            })
        });
        match ignore_quoted_identifier_case {
            Some(value) if value.eq_ignore_ascii_case("false") => {}
            Some(_) => {
                return Err(Error::Config(
                    "QUOTED_IDENTIFIERS_IGNORE_CASE must be FALSE so quoted source column names \
                     retain their exact case"
                        .to_owned(),
                ));
            }
            None => {
                return Err(Error::Config(
                    "Could not determine the effective QUOTED_IDENTIFIERS_IGNORE_CASE setting"
                        .to_owned(),
                ));
            }
        }

        Ok(())
    }
}

impl<T: TokenProvider, C: StreamClient> Client<T, C> {
    /// Build a client from pre-constructed SQL and streaming clients.
    pub fn with_clients(
        sql_client: SqlClient<T>,
        stream_client: Arc<C>,
        database: String,
        schema: String,
        pipeline_id: PipelineId,
    ) -> Self {
        Self {
            sql_client: Arc::new(sql_client),
            stream_client,
            database,
            schema,
            pipeline_id,
            channels: Arc::new(RwLock::new(HashMap::new())),
            pending_durability: Arc::new(Mutex::new(PendingDurabilityState::default())),
        }
    }

    /// Returns whether this process already has an open channel for `table_id`.
    pub(super) async fn has_channel(&self, table_id: TableId) -> bool {
        self.channels.read().await.contains_key(&table_id)
    }

    /// Creates a table when needed and prepares it for initial-copy writes.
    pub(super) async fn initialize_table(
        &self,
        table_id: TableId,
        table_name: &str,
        columns: &[ColumnSchema],
    ) -> Result<()> {
        self.prepare_channel(table_id, table_name, columns, true).await
    }

    /// Opens and validates an existing table for streaming writes.
    pub(super) async fn prepare_existing_table(
        &self,
        table_id: TableId,
        table_name: &str,
        columns: &[ColumnSchema],
    ) -> Result<()> {
        self.prepare_channel(table_id, table_name, columns, false).await
    }

    /// Prepares one table and caches its validated channel.
    #[allow(clippy::map_entry)]
    async fn prepare_channel(
        &self,
        table_id: TableId,
        table_name: &str,
        columns: &[ColumnSchema],
        create_if_missing: bool,
    ) -> Result<()> {
        // Fast path: read lock, check if already set up.
        let channels = self.channels.read().await;
        if channels.contains_key(&table_id) {
            return Ok(());
        }
        drop(channels);

        // Slow path: hold write lock for the entire setup. This runs once
        // per table per process lifetime, so blocking other tables briefly
        // during startup is acceptable.
        let mut channels = self.channels.write().await;
        if channels.contains_key(&table_id) {
            return Ok(());
        }

        schema::validate_no_cdc_collisions(columns)?;
        if create_if_missing {
            let column_defs = schema::build_column_defs(columns);
            self.sql_client.create_table_if_not_exists(table_name, &column_defs).await?;
        }
        self.validate_table(table_name, columns).await?;

        // Obtain table channel.
        let mut handle = ChannelHandle::new(
            Arc::clone(&self.stream_client),
            self.pipeline_id,
            self.database.clone(),
            self.schema.clone(),
            table_name.to_owned(),
        );
        handle.open().await?;

        // Persist table-channel mapping.
        channels.insert(table_id, Arc::new(Mutex::new(handle)));
        Ok(())
    }

    /// Validates a table before opening or reopening its streaming channel.
    ///
    /// Physical validation intentionally runs only at channel lifecycle
    /// boundaries, not during ordinary writes.
    async fn validate_table(&self, table_name: &str, columns: &[ColumnSchema]) -> Result<()> {
        let expected_column_names = columns
            .iter()
            .map(|column| column.name.as_str())
            .chain([schema::CDC_OPERATION_COLUMN, schema::CDC_SEQUENCE_COLUMN])
            .collect::<Vec<_>>();
        self.sql_client.validate_table_schema(table_name, &expected_column_names).await
    }

    /// Translates a validated schema plan into Snowflake DDL in the supplied
    /// order.
    pub async fn apply_schema_plan(&self, table_name: &str, plan: &SchemaPlan) -> Result<()> {
        if plan.is_empty() {
            return Ok(());
        }

        // Translate the shared plan without revalidating names or regrouping
        // operations.
        for operation in plan.ordered_operations() {
            match operation {
                SchemaOperation::DropColumn { before_column_schema, reason: _ } => {
                    self.sql_client.drop_column(table_name, &before_column_schema.name).await?;
                }
                SchemaOperation::AddColumn { after_column_schema, reason } => {
                    if !after_column_schema.nullable {
                        warn!(
                            table_name,
                            column_name = %after_column_schema.name,
                            "adding a source not null column as nullable in snowflake; the \
                             destination schema will be more permissive"
                        );
                    }

                    let add_column_default_clause =
                        if *reason == ColumnPresenceChangeReason::ReplicationMask {
                            if after_column_schema.default_expression.is_some() {
                                warn!(
                                    table_name,
                                    column_name = %after_column_schema.name,
                                    "not applying the source default to a publication-added \
                                     snowflake column because snowflake would populate historical \
                                     destination rows; adding the column as nullable without a \
                                     default"
                                );
                            }
                            None
                        } else {
                            schema::add_column_default_clause(after_column_schema)
                        };
                    self.sql_client
                        .add_column(
                            table_name,
                            &after_column_schema.name,
                            schema::type_name(&after_column_schema.typ),
                            add_column_default_clause.as_deref(),
                        )
                        .await?;
                }
                SchemaOperation::AlterColumn { alteration } => {
                    let before = alteration.before_column_schema();
                    let after = alteration.after_column_schema();
                    match alteration.kind() {
                        ColumnAlterationKind::Rename => {
                            self.sql_client
                                .rename_column(table_name, &before.name, &after.name)
                                .await?;
                        }
                        ColumnAlterationKind::Type => {
                            warn!(
                                table_name,
                                column_name = %before.name,
                                before_data_type = before.typ.name(),
                                before_type_modifier = before.modifier,
                                after_data_type = after.typ.name(),
                                after_type_modifier = after.modifier,
                                "snowflake column type changes are currently unsupported; \
                                 subsequent schema changes and row writes may fail or behave \
                                 unpredictably until type-change support is implemented"
                            );
                        }
                        ColumnAlterationKind::Nullability => {
                            if after.nullable {
                                debug!(
                                    table_name,
                                    column_name = %before.name,
                                    "snowflake destination column is already nullable"
                                );
                            } else {
                                warn!(
                                    table_name,
                                    column_name = %before.name,
                                    "snowflake keeps source columns nullable so key-only delete \
                                     records remain writable"
                                );
                            }
                        }
                        ColumnAlterationKind::Default => match (
                            before.default_expression.as_deref(),
                            after.default_expression.as_deref(),
                        ) {
                            (Some(_), None) => warn!(
                                table_name,
                                column_name = %before.name,
                                "skipping source column default removal for snowflake because \
                                 defaults introduced by alter table add column cannot be dropped \
                                 safely"
                            ),
                            (None, Some(_)) => warn!(
                                table_name,
                                column_name = %before.name,
                                "skipping source column default addition for snowflake because \
                                 alter column set default is only supported for existing sequence \
                                 defaults"
                            ),
                            (Some(_), Some(_)) => warn!(
                                table_name,
                                column_name = %before.name,
                                "skipping source column default replacement for snowflake because \
                                 existing defaults cannot be changed safely"
                            ),
                            (None, None) => {
                                unreachable!("default alteration should change the default");
                            }
                        },
                    }
                }
            }
        }

        Ok(())
    }
    /// Applies a source truncate to Snowflake and positions the table's
    /// Snowpipe channel at `truncate_offset`.
    ///
    /// Previously accepted rows are made durable first. The channel lock is
    /// then held while the channel is opened at the truncate boundary, the
    /// table is cleared, and the channel is reopened after DDL. This makes a
    /// failed attempt safe to replay: earlier DML is covered by the channel
    /// offset, while later DML remains eligible for replay. The first open's
    /// channel lineage and row progress identify the physical SQL attempt so a
    /// restart can reconcile an outcome-unknown request without suppressing a
    /// later required truncate.
    pub async fn truncate_table(
        &self,
        table_id: TableId,
        truncate_offset: &OffsetToken,
    ) -> Result<()> {
        loop {
            self.wait_for_pending_durability().await?;
            let mut guard = self.get_channel(table_id).await?.lock_owned().await;

            // A sender holds this channel lock until it records any accepted batch
            // in `pending_durability`. If a sender finished after the wait above,
            // release the channel and drain its batch before truncating.
            if self.pending_durability.lock().await.has_target(table_id) {
                drop(guard);
                continue;
            }

            let table_name = guard.table_name().to_owned();

            // Open at the truncate offset before clearing the table. This waits for
            // any in-flight rowset to commit and invalidates the previous owner's
            // continuation token.
            let status = guard.open_at(truncate_offset).await?;
            let channel_created_on_ms = status.created_on_ms.ok_or_else(|| {
                Error::Channel(
                    "Snowflake open response omitted the channel creation timestamp.".into(),
                )
            })?;
            let request_id = TruncateAttemptIdentity {
                pipeline_id: self.pipeline_id,
                table_id,
                database: &self.database,
                schema: &self.schema,
                table: &table_name,
                truncate_offset,
                channel_created_on_ms,
                rows_inserted: status.rows_inserted,
            }
            .request_id();
            self.sql_client.truncate_table(&table_name, request_id).await?;

            // TRUNCATE may invalidate that token, so reopen at the same offset before
            // allowing another sender to acquire the channel lock.
            guard.open_at(truncate_offset).await?;
            return Ok(());
        }
    }

    /// Retires one table's process-local state before a fresh copy reset.
    ///
    /// Callers must first prevent new streaming tasks from being admitted and
    /// wait for previously admitted tasks to finish. The returned token can
    /// then be used to perform the potentially slow remote drop without
    /// leaving a pending target that refers to a removed channel.
    pub(super) async fn detach_table_for_copy(
        &self,
        table_id: TableId,
        table_name: &str,
    ) -> Result<DetachedTableForCopy<C>> {
        let mut channels = self.channels.write().await;
        let mut pending = self.pending_durability.lock().await;
        pending.discard(table_id)?;

        let channel = channels.remove(&table_id);
        pending.observe_metrics();

        Ok(DetachedTableForCopy { channel, table_name: table_name.to_owned() })
    }

    /// Drops the remote channel and table for previously detached local state.
    pub(super) async fn drop_detached_table_for_copy(
        &self,
        detached: DetachedTableForCopy<C>,
    ) -> Result<()> {
        let DetachedTableForCopy { channel, table_name } = detached;

        let drop_channel_result = if let Some(channel) = channel {
            let mut guard = channel.lock().await;
            guard.drop_channel().await
        } else {
            let mut handle = ChannelHandle::new(
                Arc::clone(&self.stream_client),
                self.pipeline_id,
                self.database.clone(),
                self.schema.clone(),
                table_name.clone(),
            );
            handle.drop_channel().await
        };
        match drop_channel_result {
            Ok(()) => {}
            Err(Error::Snowpipe(SnowpipeError::ChannelNotFound)) => {}
            Err(error) => return Err(error),
        }

        self.sql_client.drop_table(&table_name).await
    }

    /// Validates and refreshes the table's ingestion state after a schema
    /// change.
    ///
    /// Channels are reopened after ALTER TABLE so Snowpipe picks up the new
    /// column list.
    ///
    /// Ref: <https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-classic-recommendation>
    pub async fn refresh_table(&self, table_id: &TableId, columns: &[ColumnSchema]) -> Result<()> {
        self.wait_for_pending_durability().await?;
        let channel = self.get_channel(*table_id).await?;
        let mut channel = channel.lock().await;
        let table_name = channel.table_name().to_owned();
        self.validate_table(&table_name, columns).await?;
        channel.open().await.map(|_| ())
    }

    /// Send table-copy row batches and retain their pending durability target.
    pub async fn send_table_copy_batches(
        &self,
        table_id: TableId,
        batches: Vec<RowBatch>,
    ) -> Result<()> {
        self.get_channel(table_id).await?.lock().await.accept_table_copy_batches(batches).await
    }

    /// Wait until all accepted table-copy rows for one table are durable.
    pub async fn wait_for_table_copy_durability(&self, table_id: TableId) -> Result<()> {
        self.get_channel(table_id).await?.lock().await.wait_for_table_copy_durability().await
    }

    /// Send streaming row batches and record accepted-but-not-durable targets.
    pub async fn send_streaming_batches(
        &self,
        table_id: TableId,
        batches: Vec<RowBatch>,
    ) -> Result<()> {
        let channel = self.get_channel(table_id).await?;
        for batch in batches {
            if self.pending_durability.lock().await.would_exceed_limits(batch.size()) {
                self.wait_for_pending_durability().await?;
            }

            let mut channel = channel.lock().await;
            let accepted = channel.accept_streaming_batches(vec![batch]).await?;
            for accepted_batch in accepted {
                let mut pending = self.pending_durability.lock().await;
                pending.record(table_id, accepted_batch)?;
                pending.observe_metrics();
            }
        }

        Ok(())
    }

    /// Returns whether `offset` is already committed for this table's channel.
    pub async fn is_offset_committed(
        &self,
        table_id: TableId,
        offset: &OffsetToken,
    ) -> Result<bool> {
        Ok(self.get_channel(table_id).await?.lock().await.is_offset_committed(offset))
    }

    /// Returns whether pending streaming work has reached the durability wait
    /// threshold.
    pub async fn pending_durability_limits_reached(&self) -> bool {
        self.pending_durability.lock().await.limits_reached()
    }

    /// Returns whether there is no accepted-but-not-durable streaming work.
    pub async fn pending_durability_is_empty(&self) -> bool {
        self.pending_durability.lock().await.is_empty()
    }

    /// Wait until all accepted streaming rows have committed.
    pub async fn wait_for_pending_durability(&self) -> Result<()> {
        let started = Instant::now();
        let mut observed_pending = false;
        let result = self.wait_for_pending_durability_inner(&mut observed_pending).await;
        if observed_pending {
            histogram!(ETL_SNOWFLAKE_STREAMING_DURABILITY_WAIT_SECONDS)
                .record(started.elapsed().as_secs_f64());
            if result.is_err() {
                counter!(ETL_SNOWFLAKE_STREAMING_DURABILITY_WAIT_FAILURES_TOTAL).increment(1);
            }
        }

        result
    }

    async fn wait_for_pending_durability_inner(&self, observed_pending: &mut bool) -> Result<()> {
        let deadline = Instant::now() + DEFAULT_COMMIT_WAIT_TIMEOUT;

        loop {
            let targets = self.pending_durability.lock().await.targets();
            if targets.is_empty() {
                return Ok(());
            }
            *observed_pending = true;

            let mut committed = Vec::new();
            for (table_id, target) in targets {
                let channel = self.get_channel(table_id).await?;
                if channel.lock().await.check_durability(&target, deadline).await? {
                    committed.push((table_id, target.target_offset.clone()));
                }
            }

            {
                let mut pending = self.pending_durability.lock().await;
                pending.clear_committed(&committed)?;
                pending.observe_metrics();
                if pending.is_empty() {
                    return Ok(());
                }
            }

            if Instant::now() >= deadline {
                return Err(Error::Channel(
                    "Timed out waiting for Snowflake streaming rows to commit.".into(),
                ));
            }

            sleep(DEFAULT_COMMIT_POLL_INTERVAL).await;
        }
    }

    /// Fetches the latest committed offset for this table's channel.
    pub async fn fetch_committed_offset(&self, table_id: TableId) -> Result<Option<OffsetToken>> {
        self.get_channel(table_id).await?.lock().await.fetch_committed_offset().await
    }

    /// Get table-level guard.
    ///
    /// Look up a channel by `table_id`, clone the `Arc`, and release the map
    /// read-lock before returning. The caller then locks the per-channel mutex.
    async fn get_channel(&self, table_id: TableId) -> Result<Arc<Mutex<ChannelHandle<C>>>> {
        let channels = self.channels.read().await;
        channels
            .get(&table_id)
            .cloned()
            .ok_or_else(|| Error::Channel(format!("no open channel for table {table_id}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct UnusedTokenProvider;

    impl TokenProvider for UnusedTokenProvider {
        async fn get_token(&self) -> Result<String> {
            Err(Error::Auth("Unused test token provider.".to_owned()))
        }

        async fn invalidate_token(&self) {}
    }

    fn accepted_batch(lsn: u64, ordinal: u64, rows: u64, bytes: usize) -> AcceptedRowBatch {
        AcceptedRowBatch {
            target_offset: OffsetToken::new(lsn.into(), ordinal),
            rows,
            bytes,
            baseline_rows_inserted: 100,
            channel_created_on_ms: 1,
        }
    }

    fn test_client() -> Client<UnusedTokenProvider, RestStreamClient<UnusedTokenProvider>> {
        let config = Config::new("example-account", "test-user", "test-db", "test-schema").unwrap();
        let auth = Arc::new(UnusedTokenProvider);
        let http = reqwest::Client::new();
        let sql_client =
            SqlClient::new(config.clone_without_credentials(), Arc::clone(&auth), http.clone());
        let stream_client =
            Arc::new(RestStreamClient::new(config.account_url().to_owned(), auth, http));

        Client::with_clients(
            sql_client,
            stream_client,
            config.database().to_owned(),
            config.schema().to_owned(),
            PipelineId::from(1_u64),
        )
    }

    #[test]
    fn pending_state_collapses_targets_per_channel() {
        let table_id = TableId::new(1);
        let mut state = PendingDurabilityState::default();

        state.record(table_id, accepted_batch(10, 1, 2, 20)).unwrap();
        state.record(table_id, accepted_batch(10, 2, 3, 30)).unwrap();

        let target = state.targets.get(&table_id).unwrap();
        assert_eq!(state.targets.len(), 1);
        assert_eq!(state.row_batches, 2);
        assert_eq!(state.bytes, 50);
        assert_eq!(target.target_offset, OffsetToken::new(10_u64.into(), 2));
        assert_eq!(target.rows, 5);
        assert_eq!(target.bytes, 50);
        assert_eq!(target.baseline_rows_inserted, 100);
    }

    #[test]
    fn pending_state_clears_committed_channels_from_global_totals() {
        let first_table_id = TableId::new(1);
        let second_table_id = TableId::new(2);
        let mut state = PendingDurabilityState::default();

        state.record(first_table_id, accepted_batch(10, 1, 2, 20)).unwrap();
        state.record(second_table_id, accepted_batch(11, 1, 3, 30)).unwrap();
        state.clear_committed(&[(first_table_id, OffsetToken::new(10_u64.into(), 1))]).unwrap();

        assert_eq!(state.targets.len(), 1);
        assert!(state.targets.contains_key(&second_table_id));
        assert_eq!(state.row_batches, 1);
        assert_eq!(state.bytes, 30);
    }

    #[test]
    fn pending_state_keeps_target_that_advanced_after_wait_snapshot() {
        let table_id = TableId::new(1);
        let mut state = PendingDurabilityState::default();

        state.record(table_id, accepted_batch(10, 1, 2, 20)).unwrap();
        let stale_committed_target = (table_id, OffsetToken::new(10_u64.into(), 1));
        state.record(table_id, accepted_batch(10, 2, 3, 30)).unwrap();
        state.clear_committed(&[stale_committed_target]).unwrap();

        let target = state.targets.get(&table_id).unwrap();
        assert_eq!(target.target_offset, OffsetToken::new(10_u64.into(), 2));
        assert_eq!(target.rows, 5);
        assert_eq!(target.bytes, 50);
        assert_eq!(state.row_batches, 2);
        assert_eq!(state.bytes, 50);
    }

    #[test]
    fn pending_window_allows_single_oversized_batch_when_empty() {
        let table_id = TableId::new(1);
        let mut state = PendingDurabilityState::default();

        assert!(!state.would_exceed_limits(STREAMING_PENDING_MAX_BYTES + 1));

        state.record(table_id, accepted_batch(10, 1, 1, STREAMING_PENDING_MAX_BYTES + 1)).unwrap();

        assert!(state.limits_reached());
        assert!(state.would_exceed_limits(1));
    }

    #[test]
    fn truncate_attempt_request_id_has_stable_encoding() {
        let truncate_offset = OffsetToken::new(0x1234_5678_9abc_def0_u64.into(), 42);
        let identity = TruncateAttemptIdentity {
            pipeline_id: 17,
            table_id: TableId::new(23),
            database: "TEST_DB",
            schema: "PUBLIC",
            table: "USERS",
            truncate_offset: &truncate_offset,
            channel_created_on_ms: 1_754_321_098_765,
            rows_inserted: 987_654,
        };

        assert_eq!(identity.request_id().to_string(), "f57cbf52-002a-5509-a5e7-670dd1ec603b");
    }

    #[test]
    fn truncate_attempt_request_id_changes_with_each_identity_component() {
        let truncate_offset = OffsetToken::new(100_u64.into(), 3);
        let other_truncate_offset = OffsetToken::new(100_u64.into(), 4);
        let identity = TruncateAttemptIdentity {
            pipeline_id: 17,
            table_id: TableId::new(23),
            database: "TEST_DB",
            schema: "PUBLIC",
            table: "USERS",
            truncate_offset: &truncate_offset,
            channel_created_on_ms: 1_754_321_098_765,
            rows_inserted: 987_654,
        };
        let expected = identity.request_id();
        let variants = [
            TruncateAttemptIdentity { pipeline_id: 18, ..identity },
            TruncateAttemptIdentity { table_id: TableId::new(24), ..identity },
            TruncateAttemptIdentity { database: "OTHER_DB", ..identity },
            TruncateAttemptIdentity { schema: "PRIVATE", ..identity },
            TruncateAttemptIdentity { table: "users", ..identity },
            TruncateAttemptIdentity { truncate_offset: &other_truncate_offset, ..identity },
            TruncateAttemptIdentity { channel_created_on_ms: 1_754_321_098_766, ..identity },
            TruncateAttemptIdentity { rows_inserted: 987_655, ..identity },
        ];

        for variant in variants {
            assert_ne!(variant.request_id(), expected);
        }
    }

    #[test]
    fn truncate_attempt_target_components_are_length_delimited() {
        let truncate_offset = OffsetToken::new(100_u64.into(), 3);
        let first = TruncateAttemptIdentity {
            pipeline_id: 17,
            table_id: TableId::new(23),
            database: "AB",
            schema: "C",
            table: "USERS",
            truncate_offset: &truncate_offset,
            channel_created_on_ms: 1_754_321_098_765,
            rows_inserted: 987_654,
        };
        let second = TruncateAttemptIdentity { database: "A", schema: "BC", ..first };

        assert_ne!(first.request_id(), second.request_id());
    }

    #[tokio::test]
    async fn detach_table_for_copy_retires_channel_and_pending_target() {
        let table_id = TableId::new(1);
        let table_name = "TEST_TABLE";
        let client = test_client();
        let channel = Arc::new(Mutex::new(ChannelHandle::new(
            Arc::clone(&client.stream_client),
            client.pipeline_id,
            client.database.clone(),
            client.schema.clone(),
            table_name.to_owned(),
        )));
        client.channels.write().await.insert(table_id, channel);
        client
            .pending_durability
            .lock()
            .await
            .record(table_id, accepted_batch(10, 1, 2, 20))
            .unwrap();

        let detached = client.detach_table_for_copy(table_id, table_name).await.unwrap();

        assert!(detached.channel.is_some());
        assert_eq!(detached.table_name, table_name);
        assert!(!client.channels.read().await.contains_key(&table_id));
        let pending = client.pending_durability.lock().await;
        assert!(pending.is_empty());
        assert_eq!(pending.row_batches, 0);
        assert_eq!(pending.bytes, 0);
    }
}
