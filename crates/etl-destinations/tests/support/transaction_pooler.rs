//! A local Postgres protocol proxy that enforces transaction-pooler
//! compatibility.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use url::Url;

/// Protocol observations shared by all guarded connections.
#[derive(Default)]
struct ProtocolStats {
    violations: AtomicUsize,
    preparations: AtomicUsize,
    replay_epoch_queries: AtomicUsize,
    /// Foreground sort-order reads observed on pooled connections.
    sort_order_queries: AtomicUsize,
}

/// Rejects untracked startup parameters, named statements, and preparation
/// that releases a backend before bind.
///
/// Unlike a lightly loaded pooler, this detects the prepare/execute gap on
/// every connection. The upstream must be a disposable local server accepting
/// plaintext.
pub struct TransactionPoolerGuard {
    url: Url,
    stats: Arc<ProtocolStats>,
    stop: CancellationToken,
    task: AbortOnDropHandle<()>,
}

impl TransactionPoolerGuard {
    /// Starts a protocol guard in front of a local Postgres server or pooler.
    pub async fn start(upstream: &Url) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut url = upstream.clone();
        url.set_host(Some("127.0.0.1")).unwrap();
        url.set_port(Some(listener.local_addr().unwrap().port())).unwrap();
        url.set_query(Some("sslmode=disable"));
        let upstream = (upstream.host_str().unwrap().to_owned(), upstream.port().unwrap_or(5432));
        let stats = Arc::new(ProtocolStats::default());
        let task_stats = Arc::clone(&stats);
        let stop = CancellationToken::new();
        let task_stop = stop.clone();
        let task = AbortOnDropHandle::new(tokio::spawn(async move {
            let mut clients = JoinSet::new();
            loop {
                tokio::select! {
                    () = task_stop.cancelled() => break,

                    accepted = listener.accept() => {
                        let (client, _) = accepted.unwrap();
                        let upstream = upstream.clone();
                        let stats = Arc::clone(&task_stats);
                        clients.spawn(async move {
                            let server = TcpStream::connect(upstream).await.unwrap();
                            if let Err(error) = guard_connection(client, server, stats).await {
                                assert!(matches!(error.kind(), io::ErrorKind::UnexpectedEof
                                    | io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe));
                            }
                        });
                    }

                    Some(result) = clients.join_next() => result.unwrap(),
                }
            }
            clients.abort_all();
            while let Some(result) = clients.join_next().await {
                if let Err(error) = result {
                    assert!(error.is_cancelled());
                }
            }
        }));
        Self { url, stats, stop, task }
    }

    /// Returns the guarded endpoint, preserving the upstream's database
    /// identity.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Returns the number of statements prepared through the pooled endpoint.
    pub fn prepared_queries(&self) -> usize {
        self.stats.preparations.load(Ordering::Relaxed)
    }

    /// Returns the number of pooled statements accessing replay-epoch state.
    pub fn replay_epoch_queries(&self) -> usize {
        self.stats.replay_epoch_queries.load(Ordering::Relaxed)
    }

    /// Returns the number of pooled statements accessing foreground sort state.
    pub fn sort_order_queries(&self) -> usize {
        self.stats.sort_order_queries.load(Ordering::Relaxed)
    }

    /// Joins all connections and verifies that no query required session state.
    pub async fn shutdown(self) {
        self.stop.cancel();
        self.task.await.unwrap();
        assert_eq!(self.stats.violations.load(Ordering::Relaxed), 0);
    }
}

/// Reads a length-prefixed Postgres message body, including its length header.
async fn read_body(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let length = reader.read_u32().await?;
    if !(4..=16 * 1024 * 1024).contains(&length) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid test protocol frame"));
    }
    let mut body = vec![0; usize::try_from(length).unwrap()];
    body[..4].copy_from_slice(&length.to_be_bytes());
    reader.read_exact(&mut body[4..]).await?;
    Ok(body)
}

/// Forwards protocol frames while checking the backend's transaction
/// boundaries.
async fn forward_messages(
    mut reader: impl AsyncRead + Unpin,
    mut writer: impl AsyncWrite + Unpin,
    from_server: bool,
    stats: Arc<ProtocolStats>,
) -> io::Result<()> {
    let mut prepared = false;
    loop {
        let tag = reader.read_u8().await?;
        let body = read_body(&mut reader).await?;
        if !from_server && tag == b'P' {
            stats.preparations.fetch_add(1, Ordering::Relaxed);
            let mut fields = body[4..].split(|byte| *byte == 0);
            let _statement_name = fields.next();
            if let Some(sql) = fields.next() {
                let sql = String::from_utf8_lossy(sql);
                if sql.contains("__etl_replay_epochs") {
                    stats.replay_epoch_queries.fetch_add(1, Ordering::Relaxed);
                }
                if sql.contains("ducklake_sort_expression") {
                    stats.sort_order_queries.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        if from_server && tag == b'1' {
            prepared = true;
        }
        let released_preparation =
            from_server && tag == b'Z' && prepared && body.get(4) == Some(&b'I');
        let named_statement = !from_server && tag == b'P' && body.get(4) != Some(&0);
        if released_preparation || named_statement {
            stats.violations.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if from_server && tag == b'Z' {
            prepared = false;
        }
        writer.write_u8(tag).await?;
        writer.write_all(&body).await?;
    }
}

/// Forwards startup, then stops both directions as soon as either side closes.
async fn guard_connection(
    mut client: TcpStream,
    mut server: TcpStream,
    stats: Arc<ProtocolStats>,
) -> io::Result<()> {
    client.set_nodelay(true)?;
    server.set_nodelay(true)?;
    let startup = read_body(&mut client).await?;
    // Match PgBouncer's rejection of this untracked startup parameter.
    if startup.get(8..).is_some_and(|parameters| {
        parameters.split(|byte| *byte == 0).step_by(2).any(|key| key == b"extra_float_digits")
    }) {
        stats.violations.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    server.write_all(&startup).await?;
    let (client_read, client_write) = client.split();
    let (server_read, server_write) = server.split();
    tokio::select! {
        result = forward_messages(client_read, server_write, false, Arc::clone(&stats)) => result,

        result = forward_messages(server_read, client_write, true, stats) => result,
    }
}
