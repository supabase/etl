use std::{collections::HashMap, sync::Arc};

use etl::types::{ColumnModification, ColumnSchema, PipelineId, SchemaDiff, TableId};
use reqwest::StatusCode;
use tokio::sync::{Mutex, RwLock};

use crate::snowflake::{
    Config, Error, Result,
    auth::{AuthManager, HttpExchanger, TokenProvider},
    config::{HTTP_CONNECT_TIMEOUT, HTTP_REQUEST_TIMEOUT},
    schema,
    sql::{quote_identifier, quote_string_literal},
    sql_client::SqlClient,
    streaming::{ChannelHandle, OffsetToken, RestStreamClient, RowBatch, StreamClient},
};

type ChannelMap<C> = Arc<RwLock<HashMap<TableId, Arc<Mutex<ChannelHandle<C>>>>>>;

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
        }
    }
}

/// Convenience constructor for the default client stack.
impl Client<AuthManager<HttpExchanger>> {
    pub fn new(
        config: Config,
        auth: Arc<AuthManager<HttpExchanger>>,
        pipeline_id: PipelineId,
    ) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(HTTP_CONNECT_TIMEOUT)
            .timeout(HTTP_REQUEST_TIMEOUT)
            .build()
            .expect("failed to build HTTP client");
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
    pub async fn validate_connectivity(
        config: &Config,
        private_key_pem: &str,
        passphrase: Option<&secrecy::SecretString>,
    ) -> Result<()> {
        let auth = Arc::new(
            AuthManager::new(config, private_key_pem, passphrase)
                .map_err(|e| Error::Auth(e.to_string()))?,
        );

        let http = reqwest::Client::builder()
            .connect_timeout(HTTP_CONNECT_TIMEOUT)
            .timeout(HTTP_REQUEST_TIMEOUT)
            .build()
            .map_err(Error::HttpTransport)?;

        let sql = SqlClient::new(config.clone(), auth, http);

        // `SHOW DATABASES` runs on Cloud Services (no warehouse needed).
        let db_pattern = quote_string_literal(&config.database);
        let resp = sql.execute_statement(&format!("SHOW DATABASES LIKE {db_pattern}")).await?;
        let db_exists = resp.data.is_some_and(|rows| {
            rows.iter()
                .any(|row| row.get(1).and_then(serde_json::Value::as_str) == Some(&config.database))
        });
        if !db_exists {
            return Err(Error::DatabaseNotFound(config.database.clone()));
        }

        // `SHOW SCHEMAS` also runs on Cloud Services.
        let db_ident = quote_identifier(&config.database);
        let schema_pattern = quote_string_literal(&config.schema);
        let resp = sql
            .execute_statement(&format!(
                "SHOW SCHEMAS LIKE {schema_pattern} IN DATABASE {db_ident}"
            ))
            .await?;
        let schema_exists = resp.data.is_some_and(|rows| {
            rows.iter()
                .any(|row| row.get(1).and_then(serde_json::Value::as_str) == Some(&config.schema))
        });
        if !schema_exists {
            return Err(Error::SchemaNotFound {
                database: config.database.clone(),
                schema: config.schema.clone(),
            });
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
            if add_column_default_clause.is_none()
                && let Some(default_clause) = schema::default_clause(col)
            {
                let default_expression = default_clause.trim_start_matches(" DEFAULT ");
                self.sql_client
                    .set_column_default(table_name, &col.name, default_expression)
                    .await?;
            }
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
                        if !old_nullable && *new_nullable {
                            self.sql_client
                                .drop_column_not_null(table_name, &change.new_column.name)
                                .await?;
                        }
                    }
                    ColumnModification::Default { old_expression, new_expression } => {
                        if new_expression.is_some() {
                            if let Some(default_clause) = schema::default_clause(&change.new_column)
                            {
                                let default_expression =
                                    default_clause.trim_start_matches(" DEFAULT ");
                                self.sql_client
                                    .set_column_default(
                                        table_name,
                                        &change.new_column.name,
                                        default_expression,
                                    )
                                    .await?;
                            } else if old_expression.is_some()
                                && schema::supports_default(&change.old_column)
                            {
                                self.sql_client
                                    .drop_column_default(table_name, &change.new_column.name)
                                    .await?;
                            }
                        } else {
                            self.sql_client
                                .drop_column_default(table_name, &change.new_column.name)
                                .await?;
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

    /// Truncate the table and reset ingestion state so offsets restart.
    pub async fn truncate_table(&self, table_id: TableId, table_name: &str) -> Result<()> {
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
            Err(Error::HttpStatus { status: StatusCode::NOT_FOUND, .. }) => {}
            Err(error) => return Err(error),
        }

        self.sql_client.drop_table(table_name).await
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
        self.get_channel(*table_id).await?.lock().await.open().await
    }

    /// Send pre-encoded row batches through the table's channel.
    pub async fn insert_batches(&self, table_id: TableId, batches: Vec<RowBatch>) -> Result<()> {
        self.get_channel(table_id).await?.lock().await.process_batches(batches).await
    }

    /// Last offset committed by Snowflake for this table's channel.
    pub async fn committed_offset(&self, table_id: TableId) -> Result<Option<OffsetToken>> {
        self.get_channel(table_id).await?.lock().await.committed_offset().await
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
