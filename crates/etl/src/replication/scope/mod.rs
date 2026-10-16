//! Scope-consistent replication mode. Shared PostgreSQL transport and protocol
//! primitives also serve the ordinary per-table pipeline. For destinations
//! that publish whole snapshots and source transactions. The host supplies an
//! already secured transport; this crate owns PostgreSQL startup, query, copy
//! and replication protocols. Only a destination's durable receipt permits
//! advancing feedback.
pub mod catalog;

mod decoder;
pub mod snapshot;

pub use decoder::{Cell, Change, Column, Decoder, Relation, Transaction};

pub use crate::postgres::transport::{
    Connection, ConnectionOptions, Limits, QueryRows, Row, ServerError, Socket,
};

/// Validate a deliberately restricted installation identifier before using it
/// in SQL or a replication command. No SQL is accepted from source records.
pub fn identifier(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty()
            && value.len() <= 63
            && value.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
        "invalid PostgreSQL identifier"
    );
    Ok(())
}

pub fn lsn(value: &str) -> anyhow::Result<u64> {
    let (high, low) =
        value.split_once('/').ok_or_else(|| anyhow::anyhow!("invalid PostgreSQL position"))?;
    anyhow::ensure!(
        !high.is_empty() && high.len() <= 8 && !low.is_empty() && low.len() <= 8,
        "invalid PostgreSQL position"
    );
    Ok((u64::from(u32::from_str_radix(high, 16)?) << 32) | u64::from(u32::from_str_radix(low, 16)?))
}

pub enum CaptureEvent {
    Transaction(Transaction),
    Progress(u64),
    Heartbeat,
}

/// Complete input at one source cut. Transport keepalives are deliberately
/// absent: they neither establish source progress nor enter durable custody.
pub enum CaptureUnit {
    Transaction(Transaction),
    Progress(u64),
}

/// Distinguish a host refusal from invalid source evidence. The original
/// authorization error remains in the chain for the host's normal handling.
#[derive(Debug)]
pub struct AuthorizationFailure;
impl std::fmt::Display for AuthorizationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("source capture authorization failed")
    }
}
impl std::error::Error for AuthorizationFailure {}

/// Drive a source stream to its next complete input unit. The host owns
/// authority and supplies a fresh check before every quiet-stream exchange.
/// `durable` is already retained, never a physical WAL head or pending input.
/// Cancelling this future leaves connection cleanup to its owning session.
pub async fn capture<F, Fut>(
    connection: &mut Connection,
    sql: &mut Connection,
    decoder: &mut Decoder,
    durable: u64,
    token: &str,
    mut authorize: F,
) -> anyhow::Result<CaptureUnit>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    // Every transaction returns; only a sender that never produces selected
    // progress needs this exchange bound. A refusal leaves its slot intact.
    const MAX_IDLE_KEEPALIVES: usize = 2048;
    authorize().await.map_err(|error| error.context(AuthorizationFailure))?;
    decoder.request_progress(sql, token).await?;
    for _ in 0..MAX_IDLE_KEEPALIVES {
        match next_event(connection, decoder, durable).await? {
            CaptureEvent::Transaction(transaction) => {
                return Ok(CaptureUnit::Transaction(transaction));
            }
            CaptureEvent::Progress(through) => return Ok(CaptureUnit::Progress(through)),
            CaptureEvent::Heartbeat => {
                authorize().await.map_err(|error| error.context(AuthorizationFailure))?;
                decoder.request_progress(sql, token).await?;
                connection.feedback(durable).await?;
            }
        }
    }
    anyhow::bail!(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "PostgreSQL sender produced no selected progress",
    ))
}

#[derive(Debug)]
pub enum CaptureRefusal {
    Capacity,
    SchemaChanged,
    ContinuityLost,
    InvalidData,
    SourceAhead,
}
impl std::fmt::Display for CaptureRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Capacity => "source capture capacity exceeded",
            Self::SchemaChanged => "source schema changed",
            Self::ContinuityLost => "source continuity lost",
            Self::InvalidData => "invalid source replication input",
            Self::SourceAhead => "source acknowledgement exceeds retained input",
        })
    }
}
impl std::error::Error for CaptureRefusal {}

/// Complete source units; a transaction header or physical WAL head never
/// seals a transaction. Quiet progress is qualified only between transactions.
pub async fn next_event(
    connection: &mut Connection,
    decoder: &mut Decoder,
    start: u64,
) -> anyhow::Result<CaptureEvent> {
    use postgres_replication::protocol::ReplicationMessage;
    loop {
        let bytes = connection.receive().await?;
        let length = bytes.len();
        match crate::postgres::decode_message(bytes)
            .map_err(|error| anyhow::Error::from(error).context(CaptureRefusal::InvalidData))?
        {
            ReplicationMessage::XLogData(body) => {
                if let Some(transaction) = decoder
                    .push_message(body.data(), length.saturating_sub(25))
                    .map_err(|error| {
                        if error.is::<CaptureRefusal>() {
                            error
                        } else {
                            error.context(CaptureRefusal::InvalidData)
                        }
                    })?
                {
                    return Ok(CaptureEvent::Transaction(transaction));
                }
                if let Some(through) = decoder.take_progress() {
                    anyhow::ensure!(through > start, "source barrier did not advance");
                    return Ok(CaptureEvent::Progress(through));
                }
            }
            ReplicationMessage::PrimaryKeepAlive(_) => return Ok(CaptureEvent::Heartbeat),
            _ => anyhow::bail!("unsupported PostgreSQL replication envelope"),
        }
    }
}

pub mod pipeline;
