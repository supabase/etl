use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use futures::SinkExt;
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

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
                "failed to connect to Realtime WebSocket",
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
                    "failed to send message to Realtime",
                    format!("error: {e}"),
                    source: e
                )
            })
    }
}

/// Manages a WebSocket connection with automatic reconnection and per-message retry.
///
/// All sends are fire-and-forget: no ack is expected from Realtime.
pub struct ConnectionManager {
    connection: Option<RealtimeConnection>,
    url: String,
    api_key: String,
    max_retries: u32,
    /// Incremented on every successful reconnect so callers can detect that
    /// previously-joined channels must be re-subscribed on the new connection.
    generation: u64,
}

impl ConnectionManager {
    pub fn new(url: String, api_key: String, max_retries: u32) -> Self {
        Self {
            connection: None,
            url,
            api_key,
            max_retries,
            generation: 0,
        }
    }

    /// Returns the current connection generation.
    ///
    /// The value increments each time the connection is re-established after a
    /// failure. Callers can compare against a stored generation to detect whether
    /// previously-joined channels must be re-subscribed.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Ensures the connection is established, connecting if necessary.
    pub async fn ensure_connected(&mut self) -> EtlResult<()> {
        if self.connection.is_none() {
            self.connection = Some(RealtimeConnection::connect(&self.url, &self.api_key).await?);
        }
        Ok(())
    }

    /// Sends a message with retry on failure.
    ///
    /// On send failure, reconnects and retries up to `max_retries` times.
    /// All sends are fire-and-forget — no ack is expected from Realtime.
    pub async fn send_with_retry(&mut self, message: &str) -> EtlResult<()> {
        self.ensure_connected().await?;

        let mut last_err = None;
        for attempt in 0..=self.max_retries {
            let conn = self.connection.as_mut().expect("just ensured connected");
            match conn.send(message).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    warn!(attempt, error = %e, "send failed, reconnecting");
                    last_err = Some(e);
                    let backoff = backoff_duration(attempt);
                    sleep(backoff).await;
                    if let Err(reconnect_err) = self.reconnect().await {
                        warn!(error = %reconnect_err, "reconnect failed");
                    }
                }
            }
        }

        Err(last_err.expect("loop ran at least once"))
    }

    async fn reconnect(&mut self) -> EtlResult<()> {
        match RealtimeConnection::connect(&self.url, &self.api_key).await {
            Ok(conn) => {
                self.connection = Some(conn);
                self.generation += 1;
                Ok(())
            }
            Err(e) => {
                self.connection = None;
                Err(e)
            }
        }
    }

    /// Closes the connection gracefully.
    pub async fn close(&mut self) {
        if let Some(mut conn) = self.connection.take() {
            let _ = conn.ws.close(None).await;
        }
    }
}

fn backoff_duration(attempt: u32) -> Duration {
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
        let mgr = ConnectionManager::new("wss://example.com".into(), "key".into(), 3);
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

    /// Binds an ephemeral TCP port and returns its address.
    async fn bind_listener() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, addr)
    }

    /// Spawns a mock WebSocket server that accepts one connection, collects
    /// all messages it receives, and returns them when the client closes.
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
    async fn send_with_retry_delivers_message() {
        let (listener, addr) = bind_listener().await;
        let server = mock_server(listener).await;

        // URL must include a path so tungstenite sends `/?apikey=key` (not `?apikey=key`)
        // as the HTTP request-target, which is required to be a valid URI.
        let url = format!("ws://{addr}/");
        let mut mgr = ConnectionManager::new(url, "key".into(), 3);

        mgr.send_with_retry("hello").await.unwrap();
        mgr.close().await;

        let messages = server.await.unwrap();
        assert_eq!(messages, vec!["hello"]);
    }

    #[tokio::test]
    async fn send_with_retry_delivers_multiple_messages_in_order() {
        let (listener, addr) = bind_listener().await;
        let server = mock_server(listener).await;

        let url = format!("ws://{addr}/");
        let mut mgr = ConnectionManager::new(url, "key".into(), 3);

        mgr.send_with_retry("first").await.unwrap();
        mgr.send_with_retry("second").await.unwrap();
        mgr.send_with_retry("third").await.unwrap();
        mgr.close().await;

        let messages = server.await.unwrap();
        assert_eq!(messages, vec!["first", "second", "third"]);
    }

    #[tokio::test]
    async fn send_with_retry_fails_when_no_server_available() {
        // Port 1 is reserved and should refuse connections.
        let mut mgr = ConnectionManager::new("ws://127.0.0.1:1/".into(), "key".into(), 0);
        let result = mgr.send_with_retry("hello").await;
        assert!(result.is_err());
    }
}
