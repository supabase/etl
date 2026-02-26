use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::types::{Event, PgLsn, TableId, TableRow, TableSchema};
use tokio::sync::Mutex;
use tokio::task::AbortHandle;
use tokio::time::{Duration, interval};
use tracing::{debug, warn};

#[cfg(feature = "egress")]
use crate::egress::{PROCESSING_TYPE_STREAMING, log_processed_bytes};

use super::connection::ConnectionManager;
use super::encoding::{
    build_broadcast_message, build_heartbeat_message, build_join_message, build_topic,
    delete_payload, insert_payload, table_row_to_json, truncate_payload, update_payload,
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

pub struct RealtimeConfig {
    pub url: String,
    pub api_key: String,
    pub channel_prefix: String,
    pub private_channels: bool,
    pub insert_event: String,
    pub update_event: String,
    pub delete_event: String,
    pub max_retries: u32,
}

/// Internal mutable state, protected by a single `Mutex`.
struct RealtimeInner {
    connection: ConnectionManager,
    /// Cache of table schemas populated from `Relation` events.
    schema_cache: HashMap<TableId, Arc<TableSchema>>,
    /// Topics that have received a `phx_join` in the current connection generation.
    joined_topics: HashSet<String>,
    /// The connection generation when `joined_topics` was last populated.
    /// When `connection.generation()` advances past this value, all topics
    /// must be re-joined on the new WebSocket connection.
    joined_generation: u64,
}

impl RealtimeInner {
    fn new(url: String, api_key: String, max_retries: u32) -> Self {
        Self {
            connection: ConnectionManager::new(url, api_key, max_retries),
            schema_cache: HashMap::new(),
            joined_topics: HashSet::new(),
            joined_generation: 0,
        }
    }

    /// Returns the cached schema for `table_id`, or an error if not found.
    fn get_schema(&self, table_id: TableId) -> EtlResult<Arc<TableSchema>> {
        self.schema_cache.get(&table_id).cloned().ok_or_else(|| {
            etl_error!(
                ErrorKind::MissingTableSchema,
                "schema not found for table",
                format!("table_id={table_id}")
            )
        })
    }

    /// Ensures the channel for `topic` has been joined, sending `phx_join` if needed.
    ///
    /// If the underlying connection was re-established since the last join, all
    /// previously-joined topics are cleared and the current topic is re-joined.
    async fn ensure_joined(&mut self, topic: &str) -> EtlResult<()> {
        let current_gen = self.connection.generation();
        if self.joined_generation != current_gen {
            self.joined_topics.clear();
            self.joined_generation = current_gen;
        }

        if self.joined_topics.contains(topic) {
            return Ok(());
        }

        let join_msg = build_join_message(topic);
        self.connection.send_with_retry(&join_msg).await?;
        self.joined_topics.insert(topic.to_string());
        debug!(topic, "joined Realtime channel");
        Ok(())
    }

    /// Sends a broadcast message to `topic`, joining first if required.
    async fn broadcast(
        &mut self,
        topic: &str,
        event: &str,
        payload: serde_json::Value,
    ) -> EtlResult<usize> {
        self.ensure_joined(topic).await?;
        let msg = build_broadcast_message(topic, event, payload);
        let bytes = msg.len();
        self.connection.send_with_retry(&msg).await?;
        Ok(bytes)
    }
}

/// Destination that broadcasts replicated Postgres changes to Supabase Realtime channels.
#[derive(Clone)]
pub struct RealtimeDestination {
    channel_prefix: String,
    private_channels: bool,
    insert_event: String,
    update_event: String,
    delete_event: String,
    inner: Arc<Mutex<RealtimeInner>>,
    /// Handle to abort the background heartbeat task when this destination is dropped/shutdown.
    /// Wrapped in `Arc` so it is shared across `Clone`d instances.
    heartbeat_abort: Arc<AbortHandle>,
}

impl RealtimeDestination {
    pub fn new(config: RealtimeConfig) -> Self {
        let inner = Arc::new(Mutex::new(RealtimeInner::new(
            config.url,
            config.api_key,
            config.max_retries,
        )));

        // Spawn a background task that sends a heartbeat every 20 seconds.
        // The Realtime server closes the connection after ~25 seconds of silence,
        // so this keeps the WebSocket alive during quiet periods.
        let inner_clone = Arc::clone(&inner);
        let heartbeat_task = tokio::spawn(async move {
            let mut ticker = interval(HEARTBEAT_INTERVAL);
            ticker.tick().await; // skip the immediate first tick
            loop {
                ticker.tick().await;
                let mut guard = inner_clone.lock().await;
                let msg = build_heartbeat_message();
                if let Err(e) = guard.connection.send_with_retry(&msg).await {
                    warn!(error = %e, "heartbeat send failed");
                }
            }
        });
        let abort_handle = heartbeat_task.abort_handle();
        // Detach — the AbortHandle keeps control; the task runs independently.
        drop(heartbeat_task);

        Self {
            channel_prefix: config.channel_prefix,
            private_channels: config.private_channels,
            insert_event: config.insert_event,
            update_event: config.update_event,
            delete_event: config.delete_event,
            inner,
            heartbeat_abort: Arc::new(abort_handle),
        }
    }
}

impl Destination for RealtimeDestination {
    fn name() -> &'static str {
        "realtime"
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.heartbeat_abort.abort();
        let mut inner = self.inner.lock().await;
        inner.connection.close().await;
        Ok(())
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        let schema = inner.get_schema(table_id)?;
        let topic = build_topic(&schema.name, &self.channel_prefix, self.private_channels);
        let commit_lsn = PgLsn::from(0u64);
        let payload = truncate_payload(&schema.name, commit_lsn);
        inner.broadcast(&topic, &self.delete_event, payload).await?;
        Ok(())
    }

    async fn write_table_rows(
        &self,
        _table_id: TableId,
        _table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        #[cfg_attr(not(feature = "egress"), allow(unused_variables))]
        let mut bytes_sent: u64 = 0;

        for event in events {
            match event {
                Event::Relation(rel) => {
                    inner
                        .schema_cache
                        .insert(rel.table_schema.id, Arc::new(rel.table_schema));
                }
                Event::Insert(ins) => {
                    let schema = inner.get_schema(ins.table_id)?;
                    let topic =
                        build_topic(&schema.name, &self.channel_prefix, self.private_channels);
                    let record = table_row_to_json(&ins.table_row, &schema);
                    let payload = insert_payload(&schema.name, record, ins.commit_lsn);
                    bytes_sent +=
                        inner.broadcast(&topic, &self.insert_event, payload).await? as u64;
                }
                Event::Update(upd) => {
                    let schema = inner.get_schema(upd.table_id)?;
                    let topic =
                        build_topic(&schema.name, &self.channel_prefix, self.private_channels);
                    let record = table_row_to_json(&upd.table_row, &schema);
                    let old_record = upd
                        .old_table_row
                        .as_ref()
                        .map(|(_, row)| table_row_to_json(row, &schema));
                    let payload = update_payload(&schema.name, record, old_record, upd.commit_lsn);
                    bytes_sent +=
                        inner.broadcast(&topic, &self.update_event, payload).await? as u64;
                }
                Event::Delete(del) => {
                    let schema = inner.get_schema(del.table_id)?;
                    let topic =
                        build_topic(&schema.name, &self.channel_prefix, self.private_channels);
                    let old_record = del
                        .old_table_row
                        .as_ref()
                        .map(|(_, row)| table_row_to_json(row, &schema));
                    let payload = delete_payload(&schema.name, old_record, del.commit_lsn);
                    bytes_sent +=
                        inner.broadcast(&topic, &self.delete_event, payload).await? as u64;
                }
                Event::Truncate(trunc) => {
                    for rel_id in &trunc.rel_ids {
                        let table_id = TableId::new(*rel_id);
                        match inner.get_schema(table_id) {
                            Ok(schema) => {
                                let topic = build_topic(
                                    &schema.name,
                                    &self.channel_prefix,
                                    self.private_channels,
                                );
                                let payload = truncate_payload(&schema.name, trunc.commit_lsn);
                                bytes_sent += inner
                                    .broadcast(&topic, &self.delete_event, payload)
                                    .await? as u64;
                            }
                            Err(e) => {
                                warn!(table_id = %table_id, error = %e, "skipping truncate for unknown table");
                            }
                        }
                    }
                }
                Event::Begin(_) | Event::Commit(_) | Event::Unsupported => {}
            }
        }

        #[cfg(feature = "egress")]
        if bytes_sent > 0 {
            log_processed_bytes(Self::name(), PROCESSING_TYPE_STREAMING, bytes_sent, 0);
        }

        Ok(())
    }
}
