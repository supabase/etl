use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use anyhow::Context as _;
use bytes::Bytes;
use futures::{SinkExt, StreamExt, TryStreamExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_postgres::{
    Config, CopyBothDuplex, NoTls, SimpleQueryMessage, SimpleQueryStream, config::ReplicationMode,
};

pub trait Socket: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Socket for T {}

pub type Row = Vec<Option<String>>;

#[derive(Clone, Copy)]
pub struct Limits {
    pub message_bytes: usize,
    pub query_rows: usize,
    pub io_timeout: Duration,
}

/// Deliberately no Debug: credentials never enter diagnostics.
pub struct ConnectionOptions {
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub replication: bool,
}

#[derive(Debug)]
pub struct ServerError {
    pub state: String,
}
impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PostgreSQL server refused request ({})", self.state)
    }
}
impl std::error::Error for ServerError {}

fn frame_error(failure: &AtomicU8) -> Option<anyhow::Error> {
    match failure.load(Ordering::Relaxed) {
        1 => Some(crate::replication::scope::CaptureRefusal::Capacity.into()),
        2 => Some(
            io::Error::new(io::ErrorKind::InvalidData, "invalid PostgreSQL frame length").into(),
        ),
        _ => None,
    }
}

fn error(error: tokio_postgres::Error, failure: &AtomicU8) -> anyhow::Error {
    if let Some(error) = frame_error(failure) {
        return error;
    }
    match error.code() {
        Some(code) => ServerError { state: code.code().to_owned() }.into(),
        None => {
            io::Error::new(io::ErrorKind::ConnectionAborted, "PostgreSQL connection failed").into()
        }
    }
}

/// The driver and every protocol stream have one lifetime. Dropping the owner
/// aborts the driver, including while a server response is incomplete.
pub struct Connection {
    pub(crate) snapshot: Option<(String, u64, Vec<u32>)>,
    client: tokio_postgres::Client,
    _driver: tokio::sync::watch::Receiver<super::client::PostgresConnectionUpdate>,
    stream: Option<Pin<Box<CopyBothDuplex<Bytes>>>>,
    limits: Limits,
    applied: u64,
    frame_failure: Arc<AtomicU8>,
}

pub struct QueryRows {
    stream: Pin<Box<SimpleQueryStream>>,
    rows: usize,
    bytes: usize,
    maximum: usize,
    complete: bool,
}

impl QueryRows {
    pub fn rows(&self) -> usize {
        self.rows
    }

    pub async fn next(&mut self, connection: &mut Connection) -> anyhow::Result<Option<Row>> {
        if self.complete {
            return Ok(None);
        }
        loop {
            let next = tokio::time::timeout(connection.limits.io_timeout, self.stream.try_next())
                .await
                .context("PostgreSQL query deadline exceeded")?
                .map_err(|failure| error(failure, &connection.frame_failure))?;
            match next {
                Some(SimpleQueryMessage::Row(row)) => {
                    anyhow::ensure!(
                        self.rows < self.maximum,
                        crate::replication::scope::CaptureRefusal::Capacity
                    );
                    let values: Row =
                        (0..row.len()).map(|i| row.get(i).map(str::to_owned)).collect();
                    self.bytes = self
                        .bytes
                        .checked_add(values.iter().flatten().map(String::len).sum::<usize>())
                        .context("PostgreSQL query size overflow")?;

                    self.rows += 1;
                    return Ok(Some(values));
                }
                Some(
                    SimpleQueryMessage::CommandComplete(_) | SimpleQueryMessage::RowDescription(_),
                ) => {}
                Some(_) => anyhow::bail!("unsupported PostgreSQL query response"),
                None => {
                    self.complete = true;
                    return Ok(None);
                }
            }
        }
    }
}

impl Connection {
    pub async fn connect(
        options: ConnectionOptions,
        socket: Box<dyn Socket>,
        limits: Limits,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            limits.message_bytes >= 4 && limits.query_rows > 0,
            "invalid PostgreSQL transport limits"
        );
        let mut config = Config::new();
        config.user(&options.user).dbname(&options.database);
        if let Some(password) = options.password {
            config.password(password);
        }
        if options.replication {
            config.replication_mode(ReplicationMode::Logical);
        }
        config.options(
            "-c timezone=UTC -c datestyle=ISO,YMD -c bytea_output=hex -c extra_float_digits=3 -c \
             intervalstyle=iso_8601",
        );
        // TLS negotiation and address authorization belong to the host. NoTls
        // here means the supplied stream is already authenticated/encrypted.
        let frame_failure = Arc::new(AtomicU8::new(0));
        let bounded = BoundedSocket::new(socket, limits.message_bytes, frame_failure.clone());
        let (client, connection) =
            Box::pin(tokio::time::timeout(limits.io_timeout, config.connect_raw(bounded, NoTls)))
                .await
                .context("PostgreSQL authentication deadline exceeded")?
                .map_err(|failure| error(failure, &frame_failure))?;
        let driver = super::client::spawn_postgres_connection(connection);
        Ok(Self {
            snapshot: None,
            client,
            _driver: driver,
            stream: None,
            limits,
            applied: 0,
            frame_failure,
        })
    }

    pub async fn query_rows(&mut self, sql: &str, maximum: usize) -> anyhow::Result<QueryRows> {
        anyhow::ensure!(
            self.stream.is_none() && maximum > 0 && maximum <= self.limits.query_rows,
            "PostgreSQL query is outside its connection or row bound"
        );
        let stream =
            tokio::time::timeout(self.limits.io_timeout, self.client.simple_query_raw(sql))
                .await
                .context("PostgreSQL query deadline exceeded")?
                .map_err(|failure| error(failure, &self.frame_failure))?;
        Ok(QueryRows { stream: Box::pin(stream), rows: 0, bytes: 0, maximum, complete: false })
    }

    pub async fn query(&mut self, sql: &str) -> anyhow::Result<Vec<Row>> {
        let mut rows = self.query_rows(sql, self.limits.query_rows).await?;
        let mut result = Vec::new();
        while let Some(row) = rows.next(self).await? {
            anyhow::ensure!(
                rows.bytes <= self.limits.message_bytes,
                crate::replication::scope::CaptureRefusal::Capacity
            );
            result.push(row);
        }
        Ok(result)
    }

    pub async fn start_replication(
        &mut self,
        slot: &str,
        publication: &str,
        start: u64,
    ) -> anyhow::Result<()> {
        crate::replication::scope::identifier(slot)?;
        crate::replication::scope::identifier(publication)?;
        anyhow::ensure!(self.stream.is_none(), "PostgreSQL replication already started");
        let stream = tokio::time::timeout(
            self.limits.io_timeout,
            super::client::start_replication_stream(&self.client, publication, slot, start.into()),
        )
        .await
        .context("PostgreSQL replication startup deadline exceeded")?
        .map_err(|failure| error(failure, &self.frame_failure))?;
        self.stream = Some(Box::pin(stream));
        self.applied = 0;
        Ok(())
    }

    /// A cancellation-safe read: CopyBothDuplex and its connection driver own
    /// partial frames independently of this future.
    pub async fn receive(&mut self) -> anyhow::Result<Bytes> {
        let stream = self.stream.as_mut().context("PostgreSQL replication is not running")?;
        match tokio::time::timeout(self.limits.io_timeout, stream.next())
            .await
            .context("PostgreSQL replication read deadline exceeded")?
        {
            Some(result) => result.map_err(|failure| error(failure, &self.frame_failure)),
            None => Err(frame_error(&self.frame_failure).unwrap_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "PostgreSQL replication ended").into()
            })),
        }
    }

    /// The caller must prove durable custody before supplying this position.
    /// Repeating a previous acknowledgement keeps the connection alive.
    pub async fn feedback(&mut self, durable: u64) -> anyhow::Result<()> {
        anyhow::ensure!(durable >= self.applied, "PostgreSQL acknowledgement regressed");
        let micros =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_micros();
        let postgres_micros = i64::try_from(micros)?
            .checked_sub(946684800000000)
            .context("PostgreSQL clock overflow")?;
        let message = super::stream::status_update(durable, durable, postgres_micros, 0);
        let stream = self.stream.as_mut().context("PostgreSQL replication is not running")?;
        tokio::time::timeout(self.limits.io_timeout, stream.send(message))
            .await
            .context("PostgreSQL feedback deadline exceeded")?
            .map_err(|failure| error(failure, &self.frame_failure))?;
        self.applied = durable;
        Ok(())
    }

    pub async fn close_replication(&mut self) -> anyhow::Result<()> {
        if let Some(mut stream) = self.stream.take() {
            tokio::time::timeout(self.limits.io_timeout, stream.close())
                .await
                .context("PostgreSQL replication shutdown deadline exceeded")?
                .map_err(|failure| error(failure, &self.frame_failure))?;
        }
        Ok(())
    }
}

/// Reject an oversized backend frame before exposing its length to the
/// protocol driver's allocator. A fixed scratch buffer bounds read-ahead.
struct BoundedSocket {
    inner: Box<dyn Socket>,
    header: [u8; 5],
    header_read: usize,
    header_sent: usize,
    remaining: usize,
    maximum: usize,
    scratch: [u8; 8192],
    failed: bool,
    failure: Arc<AtomicU8>,
}

impl BoundedSocket {
    fn new(inner: Box<dyn Socket>, maximum: usize, failure: Arc<AtomicU8>) -> Self {
        Self {
            inner,
            header: [0; 5],
            header_read: 0,
            header_sent: 0,
            remaining: 0,
            maximum,
            scratch: [0; 8192],
            failed: false,
            failure,
        }
    }
}

impl AsyncRead for BoundedSocket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.failed {
            return Poll::Ready(Err(io::ErrorKind::InvalidData.into()));
        }
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        while this.header_read < 5 {
            let mut header = ReadBuf::new(&mut this.header[this.header_read..]);
            match Pin::new(&mut this.inner).poll_read(cx, &mut header) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {
                    let n = header.filled().len();
                    if n == 0 {
                        return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                    }
                    this.header_read += n;
                }
            }
            if this.header_read == 5 {
                let length = u32::from_be_bytes(this.header[1..5].try_into().unwrap()) as usize;
                if length < 4 || length > this.maximum {
                    this.failed = true;
                    this.failure
                        .store(if length > this.maximum { 1 } else { 2 }, Ordering::Relaxed);
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "PostgreSQL frame exceeds its byte bound",
                    )));
                }
                this.remaining = length - 4;
            }
        }
        if this.header_sent < 5 {
            let n = output.remaining().min(5 - this.header_sent);
            output.put_slice(&this.header[this.header_sent..this.header_sent + n]);
            this.header_sent += n;
        } else if this.remaining > 0 {
            let maximum = output.remaining().min(this.remaining).min(this.scratch.len());
            let mut scratch = ReadBuf::new(&mut this.scratch[..maximum]);
            match Pin::new(&mut this.inner).poll_read(cx, &mut scratch) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {
                    let n = scratch.filled().len();
                    if n == 0 {
                        return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                    }
                    output.put_slice(scratch.filled());
                    this.remaining -= n;
                }
            }
        }
        if this.header_sent == 5 && this.remaining == 0 {
            this.header_read = 0;
            this.header_sent = 0;
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for BoundedSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
