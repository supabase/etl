use std::collections::BTreeMap;

use anyhow::Context;
use bytes::Bytes;
use postgres_replication::protocol::{
    LogicalReplicationMessage as Message, ReplicaIdentity, Tuple, TupleData,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub key: bool,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relation {
    pub oid: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<Column>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Cell {
    Null,
    Unchanged,
    Text(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Change {
    Insert { relation: u32, row: Vec<Cell> },
    Update { relation: u32, old: Option<Vec<Cell>>, row: Vec<Cell> },
    Delete { relation: u32, old: Vec<Cell> },
    Truncate { relations: Vec<u32> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transaction {
    pub xid: u32,
    pub commit_lsn: String,
    pub end_lsn: String,
    pub committed_at_micros: String,
    pub changes: Vec<Change>,
}

/// Exact textual values deliberately avoid destination-specific conversions.
/// In particular, numeric precision and array bounds survive this interface.
pub struct Decoder {
    publication: String,
    expected: BTreeMap<u32, Relation>,
    observed: BTreeMap<u32, Relation>,
    pending: Option<Transaction>,
    bytes: usize,
    maximum_bytes: usize,
    barrier: Option<(String, u64)>,
    progress: Option<u64>,
}

impl Decoder {
    pub fn new(
        publication: String,
        relations: Vec<Relation>,
        maximum_bytes: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !publication.is_empty() && !relations.is_empty() && maximum_bytes > 0,
            "empty replication scope or byte bound"
        );
        let mut expected = BTreeMap::new();
        for relation in relations {
            anyhow::ensure!(
                !relation.columns.is_empty() && relation.columns.iter().any(|c| c.key),
                "replication requires columns and row identity"
            );
            anyhow::ensure!(
                expected.insert(relation.oid, relation).is_none(),
                "duplicate replication relation"
            );
        }
        Ok(Self {
            publication,
            expected,
            observed: BTreeMap::new(),
            pending: None,
            bytes: 0,
            maximum_bytes,
            barrier: None,
            progress: None,
        })
    }

    pub fn is_idle(&self) -> bool {
        self.pending.is_none()
    }

    /// Emit at most one outstanding, nontransactional WAL marker. Observing
    /// this exact marker in the ordered stream proves quiet-source progress;
    /// the server's physical WAL head does not.
    pub async fn request_progress(
        &mut self,
        sql: &mut super::Connection,
        token: &str,
    ) -> anyhow::Result<()> {
        if self.barrier.is_some() {
            return Ok(());
        }
        anyhow::ensure!(
            token.len() == 64 && token.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid source barrier token"
        );
        let rows = sql
            .query(&format!(
                "SELECT pg_catalog.pg_logical_emit_message(false, 'etl_scope_barrier', \
                 '{token}')::text"
            ))
            .await?;
        let position = super::lsn(
            rows.first()
                .and_then(|row| row.first())
                .and_then(Option::as_deref)
                .context("source barrier position missing")?,
        )?;
        self.barrier = Some((token.to_owned(), position));
        Ok(())
    }

    /// Consume a matched, transaction-free WAL marker exactly once.
    pub fn take_progress(&mut self) -> Option<u64> {
        self.progress.take()
    }

    fn tuple(&self, oid: u32, tuple: &Tuple, key_only: bool) -> anyhow::Result<Vec<Cell>> {
        let relation =
            self.observed.get(&oid).context("change has no established source schema")?;
        let source = tuple.tuple_data();
        let keys = relation.columns.iter().filter(|c| c.key).count();
        anyhow::ensure!(
            source.len() == relation.columns.len() || (key_only && source.len() == keys),
            "tuple differs from source schema"
        );
        let sparse = source.len() != relation.columns.len();
        let mut values = source.iter();
        relation
            .columns
            .iter()
            .map(|column| {
                if sparse && !column.key {
                    return Ok(Cell::Unchanged);
                }
                Ok(match values.next().context("tuple is incomplete")? {
                    TupleData::Null => Cell::Null,
                    TupleData::UnchangedToast => Cell::Unchanged,
                    TupleData::Text(value) => Cell::Text(std::str::from_utf8(value)?.to_owned()),
                    TupleData::Binary(_) => anyhow::bail!("binary tuple was not negotiated"),
                })
            })
            .collect()
    }

    pub fn push(&mut self, data: &[u8]) -> anyhow::Result<Option<Transaction>> {
        let message = crate::postgres::codec::pgoutput::parse(&Bytes::copy_from_slice(data))?;
        self.push_message(&message, data.len())
    }

    pub(crate) fn push_message(
        &mut self,
        message: &Message,
        bytes: usize,
    ) -> anyhow::Result<Option<Transaction>> {
        self.bytes = self.bytes.checked_add(bytes).context("transaction size overflow")?;
        anyhow::ensure!(self.bytes <= self.maximum_bytes, super::CaptureRefusal::Capacity);
        let change = match message {
            Message::Begin(begin) => {
                anyhow::ensure!(self.pending.is_none(), "nested source transaction");
                self.pending = Some(Transaction {
                    xid: begin.xid(),
                    commit_lsn: begin.final_lsn().to_string(),
                    end_lsn: String::new(),
                    committed_at_micros: begin.timestamp().to_string(),
                    changes: Vec::new(),
                });
                None
            }
            Message::Relation(body) => {
                anyhow::ensure!(self.pending.is_some(), "schema outside source transaction");
                let identity = match body.replica_identity() {
                    ReplicaIdentity::Default => b'd',
                    ReplicaIdentity::Nothing => b'n',
                    ReplicaIdentity::Full => b'f',
                    ReplicaIdentity::Index => b'i',
                };
                let columns = body
                    .columns()
                    .iter()
                    .map(|c| {
                        anyhow::ensure!(matches!(c.flags(), 0 | 1), "unknown source column flags");
                        Ok(Column {
                            key: c.flags() == 1,
                            name: c.name()?.to_owned(),
                            type_oid: c.type_id() as u32,
                            type_modifier: c.type_modifier(),
                        })
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let relation = Relation {
                    oid: body.rel_id(),
                    namespace: body.namespace()?.to_owned(),
                    name: body.name()?.to_owned(),
                    replica_identity: identity,
                    columns,
                };
                anyhow::ensure!(
                    self.expected.get(&relation.oid) == Some(&relation),
                    super::CaptureRefusal::SchemaChanged
                );
                self.observed.insert(relation.oid, relation);
                None
            }
            Message::Insert(body) => {
                let row = self.tuple(body.rel_id(), body.tuple(), false)?;
                anyhow::ensure!(
                    !row.iter().any(|v| matches!(v, Cell::Unchanged)),
                    "insert has unknown values"
                );
                Some(Change::Insert { relation: body.rel_id(), row })
            }
            Message::Update(body) => Some(Change::Update {
                relation: body.rel_id(),
                old: body
                    .old_tuple()
                    .or_else(|| body.key_tuple())
                    .map(|tuple| self.tuple(body.rel_id(), tuple, body.old_tuple().is_none()))
                    .transpose()?,
                row: self.tuple(body.rel_id(), body.new_tuple(), false)?,
            }),
            Message::Delete(body) => Some(Change::Delete {
                relation: body.rel_id(),
                old: self.tuple(
                    body.rel_id(),
                    body.old_tuple()
                        .or_else(|| body.key_tuple())
                        .context("delete has no row identity")?,
                    body.old_tuple().is_none(),
                )?,
            }),
            Message::Truncate(body) => {
                anyhow::ensure!(
                    body.options() & !3 == 0 && !body.rel_ids().is_empty(),
                    "invalid source truncate"
                );
                let mut ids = std::collections::BTreeSet::new();
                for oid in body.rel_ids() {
                    anyhow::ensure!(
                        self.expected.contains_key(oid) && ids.insert(*oid),
                        "truncate is outside source scope"
                    );
                }
                Some(Change::Truncate { relations: body.rel_ids().to_vec() })
            }
            Message::Commit(body) => {
                let mut transaction =
                    self.pending.take().context("commit has no source transaction")?;
                anyhow::ensure!(
                    body.flags() == 0
                        && body.commit_lsn().to_string() == transaction.commit_lsn
                        && body.end_lsn() > body.commit_lsn()
                        && body.timestamp().to_string() == transaction.committed_at_micros,
                    "source commit differs from transaction envelope"
                );
                transaction.end_lsn = body.end_lsn().to_string();
                self.bytes = 0;
                return Ok(Some(transaction));
            }
            Message::Origin(_) | Message::Type(_) => None,
            Message::Message(body) => {
                anyhow::ensure!(matches!(body.flags(), 0 | 1), "invalid logical message flags");
                match body.prefix()? {
                    "etl_scope_barrier"
                        if self.barrier.as_ref().is_some_and(|(token, position)| {
                            token.as_bytes() == body.content().as_ref()
                                && *position == body.message_lsn()
                        }) =>
                    {
                        anyhow::ensure!(
                            body.flags() == 0 && self.pending.is_none(),
                            "source barrier interleaves a transaction"
                        );
                        self.progress = Some(body.message_lsn());
                        self.barrier = None;
                    }
                    crate::schema_capture::DDL_MESSAGE_PREFIX => {
                        let relation = crate::schema_capture::affected_table(
                            std::str::from_utf8(body.content())?,
                            &self.publication,
                        )?;
                        anyhow::ensure!(
                            body.flags() == 1 && self.pending.is_some(),
                            "schema change is not transactional"
                        );
                        // Fixed-schema admission: even an ALTER USING rewrite
                        // with unchanged column metadata invalidates this cut.
                        anyhow::ensure!(
                            relation.is_none_or(|oid| !self.expected.contains_key(&oid)),
                            super::CaptureRefusal::SchemaChanged
                        );
                    }
                    _ => {}
                }
                if self.pending.is_none() {
                    self.bytes = 0;
                }
                None
            }
            _ => anyhow::bail!("unsupported source replication message"),
        };
        if let Some(change) = change {
            self.pending.as_mut().context("change has no source transaction")?.changes.push(change);
        }
        Ok(None)
    }
}
