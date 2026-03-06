use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::types::{Event, PgLsn, TableId, TableRow, TableSchema};
use tokio::sync::{Mutex, RwLock};
use tokio::task::AbortHandle;
use tokio::time::{Duration, MissedTickBehavior, interval, sleep};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::IntervalStream;
use tracing::{debug, warn};

#[cfg(feature = "egress")]
use crate::egress::{PROCESSING_TYPE_STREAMING, log_processed_bytes};

use super::connection::{ConnectionManager, backoff_duration};
use super::encoding::{
    build_broadcast_message, build_heartbeat_message, build_join_message, build_topic,
    delete_payload, insert_payload, table_row_to_json, truncate_payload, update_payload,
};

pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const INSERT_EVENT: &str = "insert";
const UPDATE_EVENT: &str = "update";
const DELETE_EVENT: &str = "delete";

pub struct RealtimeConfig {
    pub url: String,
    pub api_key: String,
    pub private_channels: bool,
    pub max_retries: u32,
    /// How often to send a Phoenix heartbeat. Defaults to 20 s.
    pub heartbeat_interval: Duration,
}

/// Destination that broadcasts replicated Postgres changes to Supabase Realtime channels.
#[derive(Clone)]
pub struct RealtimeDestination {
    private_channels: bool,
    max_retries: u32,
    connection: Arc<Mutex<ConnectionManager>>,
    /// Shared generation counter from `ConnectionManager`. Readable without locking.
    connection_generation: Arc<AtomicU64>,
    /// Cache of table schemas populated from `Relation` events.
    schema_cache: Arc<RwLock<HashMap<TableId, Arc<TableSchema>>>>,
    /// Topics that have received a `phx_join` in the current connection generation.
    joined_topics: Arc<RwLock<HashSet<String>>>,
    /// The connection generation when `joined_topics` was last populated.
    joined_generation: Arc<AtomicU64>,
    /// Handle to abort the background heartbeat task when this destination is dropped/shutdown.
    heartbeat_abort: AbortHandle,
}

/// Sends `msg` with retry, releasing the `ConnectionManager` lock between attempts.
///
/// This avoids holding the lock during backoff sleeps, preventing the heartbeat
/// or other senders from being starved while a retry waits.
async fn send_with_retry(
    connection: &Mutex<ConnectionManager>,
    msg: &str,
    max_retries: u32,
) -> EtlResult<()> {
    let mut last_err = None;
    for attempt in 0..=max_retries {
        match connection.lock().await.try_send(msg).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                warn!(attempt, error = %e, "send failed, reconnecting");
                last_err = Some(e);
                sleep(backoff_duration(attempt)).await;
                if let Err(e) = connection.lock().await.reconnect().await {
                    warn!(error = %e, "reconnect failed");
                }
            }
        }
    }
    Err(last_err.expect("loop ran at least once"))
}

impl RealtimeDestination {
    pub fn new(config: RealtimeConfig) -> Self {
        let (manager, connection_generation) =
            ConnectionManager::new(config.url, config.api_key);
        let connection = Arc::new(Mutex::new(manager));

        let connection_clone = Arc::clone(&connection);
        let max_retries = config.max_retries;
        let heartbeat_interval = config.heartbeat_interval;
        let abort_handle = tokio::spawn(async move {
            let mut ticker = interval(heartbeat_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut stream = IntervalStream::new(ticker);
            let heartbeat = build_heartbeat_message();
            stream.next().await; // skip the immediate first tick
            loop {
                stream.next().await;
                if let Err(e) =
                    send_with_retry(&connection_clone, &heartbeat, max_retries).await
                {
                    warn!(error = %e, "heartbeat failed after all retries");
                }
            }
        })
        .abort_handle();

        Self {
            private_channels: config.private_channels,
            max_retries,
            connection,
            connection_generation,
            schema_cache: Arc::new(RwLock::new(HashMap::new())),
            joined_topics: Arc::new(RwLock::new(HashSet::new())),
            joined_generation: Arc::new(AtomicU64::new(0)),
            heartbeat_abort: abort_handle,
        }
    }

    /// Ensures the channel for `topic` has been joined, sending `phx_join` if needed.
    ///
    /// If the underlying connection was re-established since the last join, all
    /// previously-joined topics are cleared and the current topic is re-joined.
    async fn ensure_joined(&self, topic: &str) -> EtlResult<()> {
        let current_gen = self.connection_generation.load(Ordering::Relaxed);
        {
            let mut topics = self.joined_topics.write().await;
            if self.joined_generation.load(Ordering::Relaxed) != current_gen {
                topics.clear();
                self.joined_generation.store(current_gen, Ordering::Relaxed);
            }
            if !topics.insert(topic.to_string()) {
                return Ok(());
            }
        } // write lock released before network I/O
        send_with_retry(&self.connection, &build_join_message(topic), self.max_retries).await?;
        debug!(topic, "joined realtime channel");
        Ok(())
    }

    /// Sends a broadcast message to `topic`, joining first if required.
    async fn broadcast(
        &self,
        topic: &str,
        event: &str,
        payload: serde_json::Value,
    ) -> EtlResult<usize> {
        self.ensure_joined(topic).await?;
        let msg = build_broadcast_message(topic, event, payload);
        let bytes = msg.len();
        send_with_retry(&self.connection, &msg, self.max_retries).await?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::destination::Destination;
    use etl::types::{
        Cell, ColumnSchema, DeleteEvent, InsertEvent, RelationEvent, TableName, TruncateEvent,
        Type, UpdateEvent,
    };
    use futures::StreamExt;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    async fn bind_listener() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, addr)
    }

    async fn mock_server(listener: TcpListener) -> tokio::task::JoinHandle<Vec<String>> {
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let mut received = Vec::new();
            while let Some(Ok(msg)) = ws.next().await {
                match msg {
                    tokio_tungstenite::tungstenite::Message::Text(t) => {
                        received.push(t.to_string());
                    }
                    tokio_tungstenite::tungstenite::Message::Close(_) => break,
                    _ => {}
                }
            }
            received
        })
    }

    fn test_schema() -> TableSchema {
        TableSchema::new(
            TableId::new(1),
            TableName::new("public".into(), "users".into()),
            vec![ColumnSchema::new("id".into(), Type::INT4, -1, false, true)],
        )
    }

    fn test_config(url: String) -> RealtimeConfig {
        RealtimeConfig {
            url,
            api_key: "test_key".into(),
            private_channels: false,
            max_retries: 0,
            heartbeat_interval: Duration::from_secs(3600),
        }
    }

    fn lsn() -> PgLsn {
        PgLsn::from(0u64)
    }

    fn relation_event(schema: TableSchema) -> Event {
        Event::Relation(RelationEvent {
            start_lsn: lsn(),
            commit_lsn: lsn(),
            table_schema: schema,
        })
    }

    fn parsed(msg: &str) -> serde_json::Value {
        serde_json::from_str(msg).unwrap()
    }

    #[tokio::test]
    async fn inserted_row_is_broadcast_with_correct_payload() {
        let (listener, addr) = bind_listener().await;
        let server = mock_server(listener).await;
        let dest = RealtimeDestination::new(test_config(format!("ws://{addr}/")));

        dest.write_events(vec![
            relation_event(test_schema()),
            Event::Insert(InsertEvent {
                start_lsn: lsn(),
                commit_lsn: lsn(),
                table_id: TableId::new(1),
                table_row: TableRow::new(vec![Cell::I32(42)]),
            }),
        ])
        .await
        .unwrap();

        dest.shutdown().await.unwrap();
        let msgs = server.await.unwrap();

        let broadcast = msgs.iter().map(|m| parsed(m)).find(|m| m[3] == "broadcast").unwrap();
        assert_eq!(broadcast[4]["event"], "insert");
        assert_eq!(broadcast[4]["payload"]["op"], "INSERT");
        assert_eq!(broadcast[4]["payload"]["schema"], "public");
        assert_eq!(broadcast[4]["payload"]["table"], "users");
        assert_eq!(broadcast[4]["payload"]["record"]["id"], 42);
    }

    #[tokio::test]
    async fn write_events_update_broadcasts_record_and_old_record() {
        let (listener, addr) = bind_listener().await;
        let server = mock_server(listener).await;
        let dest = RealtimeDestination::new(test_config(format!("ws://{addr}/")));

        let schema = test_schema();
        dest.write_events(vec![
            relation_event(schema),
            Event::Update(UpdateEvent {
                start_lsn: lsn(),
                commit_lsn: lsn(),
                table_id: TableId::new(1),
                table_row: TableRow::new(vec![Cell::I32(2)]),
                old_table_row: Some((false, TableRow::new(vec![Cell::I32(1)]))),
            }),
        ])
        .await
        .unwrap();

        dest.shutdown().await.unwrap();
        let msgs = server.await.unwrap();

        assert_eq!(msgs.len(), 2);
        let broadcast = parsed(&msgs[1]);
        assert_eq!(broadcast[4]["event"], "update");
        assert_eq!(broadcast[4]["payload"]["op"], "UPDATE");
        assert_eq!(broadcast[4]["payload"]["record"]["id"], 2);
        assert_eq!(broadcast[4]["payload"]["old_record"]["id"], 1);
    }

    #[tokio::test]
    async fn write_events_delete_broadcasts_old_record() {
        let (listener, addr) = bind_listener().await;
        let server = mock_server(listener).await;
        let dest = RealtimeDestination::new(test_config(format!("ws://{addr}/")));

        let schema = test_schema();
        dest.write_events(vec![
            relation_event(schema),
            Event::Delete(DeleteEvent {
                start_lsn: lsn(),
                commit_lsn: lsn(),
                table_id: TableId::new(1),
                old_table_row: Some((false, TableRow::new(vec![Cell::I32(99)]))),
            }),
        ])
        .await
        .unwrap();

        dest.shutdown().await.unwrap();
        let msgs = server.await.unwrap();

        assert_eq!(msgs.len(), 2);
        let broadcast = parsed(&msgs[1]);
        assert_eq!(broadcast[4]["event"], "delete");
        assert_eq!(broadcast[4]["payload"]["op"], "DELETE");
        assert_eq!(broadcast[4]["payload"]["old_record"]["id"], 99);
        assert_eq!(broadcast[4]["payload"]["record"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn write_events_truncate_broadcasts_delete_event() {
        let (listener, addr) = bind_listener().await;
        let server = mock_server(listener).await;
        let dest = RealtimeDestination::new(test_config(format!("ws://{addr}/")));

        let schema = test_schema();
        dest.write_events(vec![
            relation_event(schema),
            Event::Truncate(TruncateEvent {
                start_lsn: lsn(),
                commit_lsn: lsn(),
                options: 0,
                rel_ids: vec![1],
            }),
        ])
        .await
        .unwrap();

        dest.shutdown().await.unwrap();
        let msgs = server.await.unwrap();

        assert_eq!(msgs.len(), 2);
        let broadcast = parsed(&msgs[1]);
        assert_eq!(broadcast[4]["event"], "delete");
        assert_eq!(broadcast[4]["payload"]["op"], "TRUNCATE");
        assert_eq!(broadcast[4]["payload"]["schema"], "public");
        assert_eq!(broadcast[4]["payload"]["table"], "users");
    }

    #[tokio::test]
    async fn write_events_insert_without_relation_returns_error() {
        let dest = RealtimeDestination::new(test_config("ws://127.0.0.1:1/".into()));

        let result = dest
            .write_events(vec![Event::Insert(InsertEvent {
                start_lsn: lsn(),
                commit_lsn: lsn(),
                table_id: TableId::new(1),
                table_row: TableRow::new(vec![Cell::I32(1)]),
            })])
            .await;

        dest.heartbeat_abort.abort();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn write_table_rows_broadcasts_as_inserts() {
        let (listener, addr) = bind_listener().await;
        let server = mock_server(listener).await;
        let dest = RealtimeDestination::new(test_config(format!("ws://{addr}/")));

        let schema = test_schema();
        dest.write_events(vec![relation_event(schema)])
            .await
            .unwrap();

        dest.write_table_rows(
            TableId::new(1),
            vec![
                TableRow::new(vec![Cell::I32(1)]),
                TableRow::new(vec![Cell::I32(2)]),
            ],
        )
        .await
        .unwrap();

        dest.shutdown().await.unwrap();
        let msgs = server.await.unwrap();

        // phx_join + 2 broadcasts
        assert_eq!(msgs.len(), 3);
        assert_eq!(parsed(&msgs[0])[3], "phx_join");
        for msg in &msgs[1..] {
            let b = parsed(msg);
            assert_eq!(b[4]["event"], "insert");
            assert_eq!(b[4]["payload"]["op"], "INSERT");
        }
        assert_eq!(parsed(&msgs[1])[4]["payload"]["record"]["id"], 1);
        assert_eq!(parsed(&msgs[2])[4]["payload"]["record"]["id"], 2);
    }

    #[tokio::test]
    async fn events_across_multiple_transactions_are_all_delivered() {
        let (listener, addr) = bind_listener().await;
        let server = mock_server(listener).await;
        let dest = RealtimeDestination::new(test_config(format!("ws://{addr}/")));

        let schema = test_schema();
        let insert = |id| Event::Insert(InsertEvent {
            start_lsn: lsn(),
            commit_lsn: lsn(),
            table_id: TableId::new(1),
            table_row: TableRow::new(vec![Cell::I32(id)]),
        });

        dest.write_events(vec![relation_event(schema.clone()), insert(1)]).await.unwrap();
        dest.write_events(vec![relation_event(schema), insert(2)]).await.unwrap();

        dest.shutdown().await.unwrap();
        let msgs = server.await.unwrap();

        let broadcasts: Vec<_> = msgs.iter().map(|m| parsed(m)).filter(|m| m[3] == "broadcast").collect();
        assert_eq!(broadcasts.len(), 2);
        assert_eq!(broadcasts[0][4]["payload"]["record"]["id"], 1);
        assert_eq!(broadcasts[1][4]["payload"]["record"]["id"], 2);
    }

    #[tokio::test]
    async fn private_channels_prefixes_topic() {
        let (listener, addr) = bind_listener().await;
        let server = mock_server(listener).await;
        let dest = RealtimeDestination::new(RealtimeConfig {
            private_channels: true,
            ..test_config(format!("ws://{addr}/"))
        });

        let schema = test_schema();
        dest.write_events(vec![
            relation_event(schema),
            Event::Insert(InsertEvent {
                start_lsn: lsn(),
                commit_lsn: lsn(),
                table_id: TableId::new(1),
                table_row: TableRow::new(vec![Cell::I32(1)]),
            }),
        ])
        .await
        .unwrap();

        dest.shutdown().await.unwrap();
        let msgs = server.await.unwrap();

        let join = parsed(&msgs[0]);
        assert_eq!(join[2], "realtime:private:etl:public.users");
    }
}

impl Destination for RealtimeDestination {
    fn name() -> &'static str {
        "realtime"
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.heartbeat_abort.abort();
        self.connection.lock().await.close().await;
        Ok(())
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        let schema = self
            .schema_cache
            .read()
            .await
            .get(&table_id)
            .cloned()
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Schema not found for table",
                    format!("table_id={table_id}")
                )
            })?;
        let topic = build_topic(&schema.name, self.private_channels);
        let payload = truncate_payload(&schema.name, PgLsn::from(0u64));
        self.broadcast(&topic, DELETE_EVENT, payload).await?;
        Ok(())
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        if table_rows.is_empty() {
            return Ok(());
        }
        let schema = self
            .schema_cache
            .read()
            .await
            .get(&table_id)
            .cloned()
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Schema not found for table",
                    format!("table_id={table_id}")
                )
            })?;
        let topic = build_topic(&schema.name, self.private_channels);
        for row in table_rows {
            let record = table_row_to_json(&row, &schema);
            let payload = insert_payload(&schema.name, record, PgLsn::from(0u64));
            self.broadcast(&topic, INSERT_EVENT, payload).await?;
        }
        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        #[cfg_attr(not(feature = "egress"), allow(unused_variables))]
        let mut bytes_sent: u64 = 0;

        for event in events {
            match event {
                Event::Relation(rel) => {
                    self.schema_cache
                        .write()
                        .await
                        .insert(rel.table_schema.id, Arc::new(rel.table_schema));
                }
                Event::Insert(ins) => {
                    let schema = self
                        .schema_cache
                        .read()
                        .await
                        .get(&ins.table_id)
                        .cloned()
                        .ok_or_else(|| {
                            etl_error!(
                                ErrorKind::MissingTableSchema,
                                "Schema not found for table",
                                format!("table_id={}", ins.table_id)
                            )
                        })?;
                    let topic =
                        build_topic(&schema.name, self.private_channels);
                    let record = table_row_to_json(&ins.table_row, &schema);
                    let payload = insert_payload(&schema.name, record, ins.commit_lsn);
                    bytes_sent +=
                        self.broadcast(&topic, INSERT_EVENT, payload).await? as u64;
                }
                Event::Update(upd) => {
                    let schema = self
                        .schema_cache
                        .read()
                        .await
                        .get(&upd.table_id)
                        .cloned()
                        .ok_or_else(|| {
                            etl_error!(
                                ErrorKind::MissingTableSchema,
                                "Schema not found for table",
                                format!("table_id={}", upd.table_id)
                            )
                        })?;
                    let topic =
                        build_topic(&schema.name, self.private_channels);
                    let record = table_row_to_json(&upd.table_row, &schema);
                    let old_record = upd
                        .old_table_row
                        .as_ref()
                        .map(|(_, row)| table_row_to_json(row, &schema));
                    let payload = update_payload(&schema.name, record, old_record, upd.commit_lsn);
                    bytes_sent +=
                        self.broadcast(&topic, UPDATE_EVENT, payload).await? as u64;
                }
                Event::Delete(del) => {
                    let schema = self
                        .schema_cache
                        .read()
                        .await
                        .get(&del.table_id)
                        .cloned()
                        .ok_or_else(|| {
                            etl_error!(
                                ErrorKind::MissingTableSchema,
                                "Schema not found for table",
                                format!("table_id={}", del.table_id)
                            )
                        })?;
                    let topic =
                        build_topic(&schema.name, self.private_channels);
                    let old_record = del
                        .old_table_row
                        .as_ref()
                        .map(|(_, row)| table_row_to_json(row, &schema));
                    let payload = delete_payload(&schema.name, old_record, del.commit_lsn);
                    bytes_sent +=
                        self.broadcast(&topic, DELETE_EVENT, payload).await? as u64;
                }
                Event::Truncate(trunc) => {
                    for rel_id in &trunc.rel_ids {
                        let table_id = TableId::new(*rel_id);
                        match self.schema_cache.read().await.get(&table_id).cloned() {
                            Some(schema) => {
                                let topic = build_topic(&schema.name, self.private_channels);
                                let payload = truncate_payload(&schema.name, trunc.commit_lsn);
                                bytes_sent += self
                                    .broadcast(&topic, DELETE_EVENT, payload)
                                    .await? as u64;
                            }
                            None => {
                                warn!(table_id = %table_id, "skipping truncate for unknown table");
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
