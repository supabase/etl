use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use futures::SinkExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time::Duration;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, info};

const BACKOFF_BASE_MS: u64 = 1_000;
const BACKOFF_MAX_MS: u64 = 30_000;

/// A live WebSocket connection to Supabase Realtime.
struct RealtimeConnection {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl RealtimeConnection {
    async fn connect(url: &str, api_key: &str) -> EtlResult<Self> {
        let connect_url = format!("{}?apikey={}&vsn=2.0.0", url, api_key);
        debug!(url = %url, "connecting to Realtime WebSocket");

        let (ws, _) = connect_async(&connect_url).await.map_err(|e| {
            etl_error!(
                ErrorKind::DestinationConnectionFailed,
                "Failed to connect to Realtime WebSocket",
                format!("URL: {url}, error: {e}"),
                source: e
            )
        })?;

        info!(url = %url, "connected to Realtime WebSocket");
        Ok(Self { ws })
    }

    async fn send(&mut self, message: &str) -> EtlResult<()> {
        self.ws
            .send(Message::Text(message.to_string().into()))
            .await
            .map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationIoError,
                    "Failed to send message to Realtime",
                    format!("error: {e}"),
                    source: e
                )
            })
    }
}

/// Manages a WebSocket connection with reconnect support.
///
/// All sends are fire-and-forget: no ack is expected from Realtime.
/// Retry logic is intentionally kept outside this struct (in `core.rs`) so that
/// the lock on this struct is not held during backoff sleeps.
pub struct ConnectionManager {
    connection: Option<RealtimeConnection>,
    url: String,
    api_key: String,
    /// Incremented on every successful reconnect so callers can detect that
    /// previously-joined channels must be re-subscribed on the new connection.
    /// Shared with [`RealtimeDestination`] via `Arc` so generation can be read
    /// without acquiring the `Mutex<ConnectionManager>`.
    generation: Arc<AtomicU64>,
}

impl ConnectionManager {
    /// Creates a new manager and returns a shared handle to the connection generation counter.
    pub fn new(url: String, api_key: String) -> (Self, Arc<AtomicU64>) {
        let generation = Arc::new(AtomicU64::new(0));
        (
            Self {
                connection: None,
                url,
                api_key,
                generation: Arc::clone(&generation),
            },
            generation,
        )
    }

    /// Ensures the connection is established, connecting if necessary.
    pub async fn ensure_connected(&mut self) -> EtlResult<()> {
        if self.connection.is_none() {
            self.connection = Some(RealtimeConnection::connect(&self.url, &self.api_key).await?);
        }
        Ok(())
    }

    /// Attempts a single send. Ensures the connection first, then sends once.
    ///
    /// On send failure the connection is cleared so the next call will reconnect.
    /// Callers are responsible for retrying with backoff.
    pub async fn try_send(&mut self, message: &str) -> EtlResult<()> {
        self.ensure_connected().await?;
        let conn = self.connection.as_mut().expect("ensure_connected guarantees Some");
        match conn.send(message).await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.connection = None;
                Err(e)
            }
        }
    }

    pub(super) async fn reconnect(&mut self) -> EtlResult<()> {
        let conn = RealtimeConnection::connect(&self.url, &self.api_key).await?;
        self.connection = Some(conn);
        self.generation.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Closes the connection gracefully.
    pub async fn close(&mut self) {
        if let Some(mut conn) = self.connection.take() {
            let _ = conn.ws.close(None).await;
        }
    }
}

pub(super) fn backoff_duration(attempt: u32) -> Duration {
    let shift = attempt.min(63) as u64;
    let ms = BACKOFF_BASE_MS
        .saturating_mul(1u64 << shift)
        .min(BACKOFF_MAX_MS);
    Duration::from_millis(ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[test]
    fn connection_manager_starts_disconnected() {
        let (mgr, _gen) = ConnectionManager::new("wss://example.com".into(), "key".into());
        assert!(mgr.connection.is_none());
    }

    #[test]
    fn backoff_duration_doubles_each_attempt() {
        assert_eq!(backoff_duration(0), Duration::from_millis(1_000));
        assert_eq!(backoff_duration(1), Duration::from_millis(2_000));
        assert_eq!(backoff_duration(2), Duration::from_millis(4_000));
        assert_eq!(backoff_duration(3), Duration::from_millis(8_000));
        assert_eq!(backoff_duration(4), Duration::from_millis(16_000));
    }

    #[test]
    fn backoff_duration_caps_at_max() {
        assert_eq!(backoff_duration(5), Duration::from_millis(30_000));
        assert_eq!(backoff_duration(10), Duration::from_millis(30_000));
        assert_eq!(backoff_duration(u32::MAX), Duration::from_millis(30_000));
    }

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

    #[tokio::test]
    async fn try_send_delivers_message() {
        let (listener, addr) = bind_listener().await;
        let server = mock_server(listener).await;

        let url = format!("ws://{addr}/");
        let (mut mgr, _gen) = ConnectionManager::new(url, "key".into());

        mgr.try_send("hello").await.unwrap();
        mgr.close().await;

        let messages = server.await.unwrap();
        assert_eq!(messages, vec!["hello"]);
    }

    #[tokio::test]
    async fn try_send_delivers_multiple_messages_in_order() {
        let (listener, addr) = bind_listener().await;
        let server = mock_server(listener).await;

        let url = format!("ws://{addr}/");
        let (mut mgr, _gen) = ConnectionManager::new(url, "key".into());

        mgr.try_send("first").await.unwrap();
        mgr.try_send("second").await.unwrap();
        mgr.try_send("third").await.unwrap();
        mgr.close().await;

        let messages = server.await.unwrap();
        assert_eq!(messages, vec!["first", "second", "third"]);
    }

    #[tokio::test]
    async fn try_send_fails_when_no_server_available() {
        let (mut mgr, _gen) = ConnectionManager::new("ws://127.0.0.1:1/".into(), "key".into());
        let result = mgr.try_send("hello").await;
        assert!(result.is_err());
    }
}
