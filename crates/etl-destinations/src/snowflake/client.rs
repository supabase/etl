use std::{collections::HashMap, sync::Arc, time::Instant};

use etl::{
    pipeline::PipelineId,
    schema::{ColumnChange, ColumnModification, ColumnSchema, SchemaDiff, TableId},
};
use metrics::{counter, gauge, histogram};
use tokio::{
    sync::{Mutex, RwLock},
    time::sleep,
};
use tracing::warn;

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
        OffsetToken, RestStreamClient, RowBatch, StreamClient, validate_committed_status,
    },
};

type ChannelMap<C> = Arc<RwLock<HashMap<TableId, Arc<Mutex<ChannelHandle<C>>>>>>;

/// Maximum accepted Snowflake row batches before forcing a durability wait.
const STREAMING_PENDING_MAX_ROW_BATCHES: usize = 64;

/// Maximum accepted compressed bytes before forcing a durability wait.
const STREAMING_PENDING_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Collapsed durability target for one Snowflake streaming channel.
///
/// Snowflake committed offsets are cumulative, so multiple accepted row
/// batches can be represented by the latest target offset plus aggregate row
/// and byte counts.
#[derive(Debug, Clone)]
struct PendingDurabilityTarget {
    /// Latest accepted offset for this channel.
    target_offset: OffsetToken,
    /// Accepted rows since the baseline status.
    rows: u64,
    /// Compressed bytes accepted since the baseline status.
    bytes: usize,
    /// Snowflake row batches accepted since the baseline status.
    row_batches: usize,
    /// Cumulative inserted rows before the pending range began.
    baseline_rows_inserted: u64,
    /// Cumulative row errors before the pending range began.
    baseline_rows_error_count: u64,
}

impl PendingDurabilityTarget {
    /// Creates a pending durability target from the first accepted row batch.
    fn new(batch: AcceptedRowBatch) -> Self {
        Self {
            target_offset: batch.target_offset,
            rows: batch.rows,
            bytes: batch.bytes,
            row_batches: 1,
            baseline_rows_inserted: batch.baseline_rows_inserted,
            baseline_rows_error_count: batch.baseline_rows_error_count,
        }
    }

    /// Extends this target with a later accepted row batch on the same channel.
    fn record(&mut self, batch: AcceptedRowBatch) -> Result<()> {
        let rows = self.rows.checked_add(batch.rows).ok_or_else(|| {
            Error::Channel("Snowflake pending streaming row count overflowed.".into())
        })?;
        let bytes = self.bytes.checked_add(batch.bytes).ok_or_else(|| {
            Error::Channel("Snowflake pending streaming byte count overflowed.".into())
        })?;
        let row_batches = self.row_batches.checked_add(1).ok_or_else(|| {
            Error::Channel("Snowflake pending streaming batch count overflowed.".into())
        })?;

        self.target_offset = batch.target_offset;
        self.rows = rows;
        self.bytes = bytes;
        self.row_batches = row_batches;

        Ok(())
    }

    /// Converts this cumulative target into the validation input shape.
    fn as_accepted_batch(&self) -> AcceptedRowBatch {
        AcceptedRowBatch {
            target_offset: self.target_offset.clone(),
            rows: self.rows,
            bytes: self.bytes,
            baseline_rows_inserted: self.baseline_rows_inserted,
            baseline_rows_error_count: self.baseline_rows_error_count,
        }
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

    /// Clones the current pending targets for status polling.
    fn targets(&self) -> Vec<(TableId, PendingDurabilityTarget)> {
        self.targets.iter().map(|(table_id, target)| (*table_id, target.clone())).collect()
    }

    /// Removes targets whose current offset is covered by the proven target.
    fn clear_committed(&mut self, committed: &[(TableId, OffsetToken)]) {
        for (table_id, committed_target) in committed {
            let should_clear = self
                .targets
                .get(table_id)
                .is_some_and(|target| &target.target_offset <= committed_target);

            if should_clear && let Some(target) = self.targets.remove(table_id) {
                self.row_batches = self.row_batches.saturating_sub(target.row_batches);
                self.bytes = self.bytes.saturating_sub(target.bytes);
            }
        }
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

    /// Ensure the table exists in Snowflake and is ready to receive data.
    ///
    /// Returns `true` when streaming was newly set up for this table in the
    /// current process (the Snowflake table itself may have already existed).
    #[allow(clippy::map_entry)]
    pub async fn ensure_table(
        &self,
        table_id: TableId,
        table_name: &str,
        columns: &[ColumnSchema],
    ) -> Result<bool> {
        // Fast path: read lock, check if already set up.
        let channels = self.channels.read().await;
        if channels.contains_key(&table_id) {
            return Ok(false);
        }
        drop(channels);

        // Slow path: hold write lock for the entire setup. This runs once
        // per table per process lifetime, so blocking other tables briefly
        // during startup is acceptable.
        let mut channels = self.channels.write().await;
        if channels.contains_key(&table_id) {
            return Ok(false);
        }

        // Create Snowflake table.
        schema::validate_no_cdc_collisions(columns)?;
        let column_defs = schema::build_column_defs(columns);
        self.sql_client.create_table_if_not_exists(table_name, &column_defs).await?;

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
        Ok(true)
    }

    /// Apply column additions, renames, updates, and removals from a schema
    /// diff.
    pub async fn apply_schema_diff(&self, table_name: &str, diff: &SchemaDiff) -> Result<()> {
        if diff.is_empty() {
            return Ok(());
        }

        for col in &diff.columns_to_add {
            let add_column_default_clause = schema::add_column_default_clause(col);
            self.sql_client
                .add_column(
                    table_name,
                    &col.name,
                    schema::type_name(&col.typ),
                    add_column_default_clause.as_deref(),
                )
                .await?;
        }

        for change in &diff.columns_to_change {
            for modification in &change.modifications {
                let ColumnModification::Rename { old_name, new_name } = modification else {
                    continue;
                };

                self.sql_client.rename_column(table_name, old_name, new_name).await?;
            }
        }

        for change in &diff.columns_to_change {
            for modification in &change.modifications {
                match modification {
                    ColumnModification::Rename { .. } => {}
                    ColumnModification::Nullability { old_nullable, new_nullable } => {
                        warn!(
                            table_name,
                            column_name = %change.new_column.name,
                            old_nullable,
                            new_nullable,
                            "skipping source column nullability change for Snowflake"
                        );
                    }
                    ColumnModification::Default { old_expression, new_expression } => {
                        if new_expression.is_some() {
                            Self::warn_skipping_column_default_change(table_name, change);
                        } else {
                            Self::warn_skipping_column_default_drop(
                                table_name,
                                change,
                                old_expression.as_deref(),
                            );
                        }
                    }
                }
            }
        }

        for col in &diff.columns_to_remove {
            self.sql_client.drop_column(table_name, &col.name).await?;
        }

        Ok(())
    }

    /// Logs that Snowflake default-change DDL is being skipped.
    fn warn_skipping_column_default_change(table_name: &str, change: &ColumnChange) {
        warn!(
            table_name,
            column_name = %change.new_column.name,
            "skipping source column default change for Snowflake because ALTER COLUMN SET DEFAULT \
             is only supported for existing sequence defaults"
        );
    }

    /// Logs that Snowflake default-drop DDL is being skipped.
    fn warn_skipping_column_default_drop(
        table_name: &str,
        change: &ColumnChange,
        old_expression: Option<&str>,
    ) {
        if old_expression.is_some_and(|default_expression| {
            schema::supports_column_default(default_expression, &change.old_column.typ)
        }) {
            warn!(
                table_name,
                column_name = %change.new_column.name,
                "skipping source column default removal for Snowflake because defaults introduced \
                 by ALTER TABLE ADD COLUMN cannot be dropped safely"
            );
        }
    }

    /// Truncate the table and reset ingestion state so offsets restart.
    pub async fn truncate_table(&self, table_id: TableId, table_name: &str) -> Result<()> {
        self.wait_for_pending_durability().await?;
        let mut guard = self.get_channel(table_id).await?.lock_owned().await;
        self.sql_client.truncate_table(table_name).await?;
        guard.reset().await
    }

    /// Drop the table and destination-private replay state before a fresh copy.
    pub async fn drop_table_for_copy(&self, table_id: TableId, table_name: &str) -> Result<()> {
        let channel = self.channels.write().await.remove(&table_id);

        let drop_channel_result = if let Some(channel) = channel {
            let mut guard = channel.lock().await;
            guard.drop_channel().await
        } else {
            let mut handle = ChannelHandle::new(
                Arc::clone(&self.stream_client),
                self.pipeline_id,
                self.database.clone(),
                self.schema.clone(),
                table_name.to_owned(),
            );
            handle.drop_channel().await
        };
        match drop_channel_result {
            Ok(()) => {}
            Err(Error::Snowpipe(SnowpipeError::ChannelNotFound)) => {}
            Err(error) => return Err(error),
        }

        self.sql_client.drop_table(table_name).await
    }

    /// Refresh the table's ingestion state after a schema change.
    ///
    /// Channels must be reopened after ALTER TABLE so Snowpipe picks up the
    /// new column list. Without this, inserts would fail (and fall back to
    /// the auto-recovery path in `accept_streaming_batches`, so after one error
    /// round-trip data would still be pushed, but we can avoid that extra
    /// trip).
    ///
    /// Ref: <https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-classic-recommendation>
    pub async fn refresh_table(&self, table_id: &TableId) -> Result<()> {
        self.wait_for_pending_durability().await?;
        self.get_channel(*table_id).await?.lock().await.open().await.map(|_| ())
    }

    /// Send table-copy row batches through the existing append-ack path.
    pub async fn send_table_copy_batches(
        &self,
        table_id: TableId,
        batches: Vec<RowBatch>,
    ) -> Result<()> {
        self.get_channel(table_id).await?.lock().await.send_table_copy_batches(batches).await
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

            let accepted = channel.lock().await.accept_streaming_batches(vec![batch]).await?;
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
                let status = channel.lock().await.refresh_status().await?;
                let accepted_batch = target.as_accepted_batch();
                validate_committed_status(&status, &accepted_batch)?;

                if status
                    .offset_token
                    .as_ref()
                    .is_some_and(|offset| offset >= &target.target_offset)
                {
                    committed.push((table_id, target.target_offset.clone()));
                }
            }

            {
                let mut pending = self.pending_durability.lock().await;
                pending.clear_committed(&committed);
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

    fn accepted_batch(lsn: u64, ordinal: u64, rows: u64, bytes: usize) -> AcceptedRowBatch {
        AcceptedRowBatch {
            target_offset: OffsetToken::new(lsn.into(), ordinal),
            rows,
            bytes,
            baseline_rows_inserted: 100,
            baseline_rows_error_count: 1,
        }
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
        assert_eq!(target.baseline_rows_error_count, 1);
    }

    #[test]
    fn pending_state_clears_committed_channels_from_global_totals() {
        let first_table_id = TableId::new(1);
        let second_table_id = TableId::new(2);
        let mut state = PendingDurabilityState::default();

        state.record(first_table_id, accepted_batch(10, 1, 2, 20)).unwrap();
        state.record(second_table_id, accepted_batch(11, 1, 3, 30)).unwrap();
        state.clear_committed(&[(first_table_id, OffsetToken::new(10_u64.into(), 1))]);

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
        state.clear_committed(&[stale_committed_target]);

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
}
