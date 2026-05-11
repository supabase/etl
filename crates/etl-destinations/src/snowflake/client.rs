use std::{collections::HashMap, sync::Arc};

use etl::types::{ColumnSchema, PipelineId, SchemaDiff, TableId};
use tokio::sync::{Mutex, RwLock};

use crate::snowflake::{
    Config, Error, Result,
    auth::{AuthManager, HttpExchanger, TokenProvider},
    schema,
    sql_client::SqlClient,
    streaming::{ChannelHandle, OffsetToken, RestStreamClient, RowBatch, StreamClient},
};

/// Snowflake API client.
///
/// Unifies the SQL REST API (DDL) and the Snowpipe Streaming API (channel
/// lifecycle and row ingestion).
pub struct SnowflakeClient<T: TokenProvider, C: StreamClient = RestStreamClient<T>> {
    sql_client: Arc<SqlClient<T>>,
    stream_client: Arc<C>,
    database: String,
    schema: String,
    pipeline_id: PipelineId,
    channels: Arc<RwLock<HashMap<TableId, Arc<Mutex<ChannelHandle<C>>>>>>,
}

impl<T: TokenProvider, C: StreamClient> Clone for SnowflakeClient<T, C> {
    fn clone(&self) -> Self {
        Self {
            sql_client: Arc::clone(&self.sql_client),
            stream_client: Arc::clone(&self.stream_client),
            database: self.database.clone(),
            schema: self.schema.clone(),
            pipeline_id: self.pipeline_id,
            channels: Arc::clone(&self.channels),
        }
    }
}

/// Convenience constructor for the default client stack.
impl SnowflakeClient<AuthManager<HttpExchanger>> {
    pub fn new(
        config: Config,
        auth: Arc<AuthManager<HttpExchanger>>,
        pipeline_id: PipelineId,
    ) -> Self {
        let http = reqwest::Client::new();
        let database = config.database.clone();
        let schema = config.schema.clone();
        let stream_client = Arc::new(RestStreamClient::new(
            config.account_url.clone(),
            Arc::clone(&auth),
            http.clone(),
        ));
        let sql_client = SqlClient::new(config, auth, http);
        Self::with_clients(sql_client, stream_client, database, schema, pipeline_id)
    }
}

impl<T: TokenProvider, C: StreamClient> SnowflakeClient<T, C> {
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
        }
    }

    /// Ensure the table exists in Snowflake and is ready to receive data.
    ///
    /// Returns `true` when the table was newly created (DDL was issued).
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

    /// Apply column additions, renames, and removals from a schema diff.
    pub async fn apply_schema_diff(&self, table_name: &str, diff: &SchemaDiff) -> Result<()> {
        if diff.is_empty() {
            return Ok(());
        }

        for col in &diff.columns_to_add {
            self.sql_client.add_column(table_name, &col.name, schema::type_name(&col.typ)).await?;
        }

        for rename in &diff.columns_to_rename {
            self.sql_client.rename_column(table_name, &rename.old_name, &rename.new_name).await?;
        }

        for col in &diff.columns_to_remove {
            self.sql_client.drop_column(table_name, &col.name).await?;
        }

        Ok(())
    }

    /// Truncate the table and reset ingestion state so offsets restart.
    pub async fn truncate_table(&self, table_id: TableId, table_name: &str) -> Result<()> {
        let mut guard = self.get_channel(table_id).await?.lock_owned().await;
        self.sql_client.truncate_table(table_name).await?;
        guard.reset().await
    }

    /// Refresh the table's ingestion state after a schema change.
    ///
    /// Channels must be reopened after ALTER TABLE so Snowpipe picks up the
    /// new column list. Without this, inserts would fail (and fall back to
    /// the auto-recovery path in `process_batches`, so after one error
    /// round-trip data would still be pushed, but we can avoid that extra
    /// trip).
    ///
    /// Ref: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-classic-recommendation
    pub async fn refresh_table(&self, table_id: &TableId) -> Result<()> {
        self.get_channel(*table_id).await?.lock_owned().await.open().await
    }

    /// Send pre-encoded row batches through the table's channel.
    pub async fn insert_batches(&self, table_id: TableId, batches: Vec<RowBatch>) -> Result<()> {
        self.get_channel(table_id).await?.lock_owned().await.process_batches(batches).await
    }

    /// Last offset committed by Snowflake for this table's channel.
    pub async fn committed_offset(&self, table_id: TableId) -> Result<Option<OffsetToken>> {
        self.get_channel(table_id).await?.lock_owned().await.committed_offset().await
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
