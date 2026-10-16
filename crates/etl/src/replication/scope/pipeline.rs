//! Whole-scope synchronization. Ordinary table mode and this mode share the
//! PostgreSQL connection owner, guarded pgoutput decoder, source helpers and
//! replication command path. Only bootstrap coordination and unit assembly
//! differ: every selected table crosses one cut before streaming begins.
use std::{future::Future, sync::Arc};

use anyhow::Context;

use super::{CaptureUnit, Cell, Connection, Decoder, Relation, Transaction, catalog, snapshot};

#[derive(Clone, Copy)]
pub enum ConnectionKind {
    Query,
    Replication,
}

/// Address policy, TLS and credentials are supplied by the embedding host.
/// The returned connection is driven and owned by ETL, like ordinary clients.
pub trait SourceFactory: Send + Sync {
    fn connect(
        &self,
        kind: ConnectionKind,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<Connection>>;
}

pub struct ScopeConfig {
    pub publication: String,
    pub slot: String,
    pub relations: Vec<Relation>,
    pub source_lock: i64,
    pub snapshot_readers: usize,
    pub partitions_per_relation: usize,
    pub fetch_rows: usize,
    pub page_bytes: usize,
    pub transaction_bytes: usize,
}

#[derive(Clone)]
pub struct ScopeCheckpoint {
    pub system_id: String,
    pub database_oid: String,
    pub timeline: u32,
    /// Complete input already retained by the destination; never visibility.
    pub captured: Option<u64>,
}

pub struct SnapshotRow {
    pub relation: u32,
    pub cells: Vec<Cell>,
}

pub enum Unit {
    Snapshot { through: u64, plan: Vec<snapshot::Partition> },
    Transaction { from: u64, transaction: Transaction },
    Progress { from: u64, through: u64 },
}

impl Unit {
    pub fn through(&self) -> anyhow::Result<u64> {
        match self {
            Self::Snapshot { through, .. } | Self::Progress { through, .. } => Ok(*through),
            Self::Transaction { transaction, .. } => Ok(transaction.end_lsn.parse()?),
        }
    }
}

/// A page is provisional. Only `seal` transfers responsibility for complete,
/// replayable input. The returned position must name that exact unit and be
/// backed by the host's authoritative durable storage.
pub trait CaptureDestination: Send + Sync {
    fn authorize(&self) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn authorize_feedback(&self, through: u64) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn snapshot_page(
        &mut self,
        partition: usize,
        rows: Vec<SnapshotRow>,
        exhausted: bool,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn seal(&mut self, unit: Unit) -> impl Future<Output = anyhow::Result<u64>> + Send;
}

pub struct ScopePipeline {
    factory: Arc<dyn SourceFactory>,
    config: ScopeConfig,
    checkpoint: ScopeCheckpoint,
    sql: Option<Connection>,
    replication: Option<Connection>,
    decoder: Option<Decoder>,
    streaming: bool,
    connected: bool,
}

impl ScopePipeline {
    pub fn new(
        factory: Arc<dyn SourceFactory>,
        config: ScopeConfig,
        checkpoint: ScopeCheckpoint,
    ) -> anyhow::Result<Self> {
        super::identifier(&config.publication)?;
        super::identifier(&config.slot)?;
        anyhow::ensure!(
            !config.relations.is_empty()
                && (1..=64).contains(&config.snapshot_readers)
                && config.fetch_rows > 0
                && config.page_bytes > 0
                && config.transaction_bytes > 0,
            "invalid scope capture limits"
        );
        Ok(Self {
            factory,
            config,
            checkpoint,
            sql: None,
            replication: None,
            decoder: None,
            streaming: false,
            connected: false,
        })
    }

    /// Resume a bounded CPU turn without reconstructing stream-local relation
    /// state. The host has revalidated its lease before lending this pipeline.
    pub fn with_connections(mut self, sql: Connection, replication: Connection) -> Self {
        self.sql = Some(sql);
        self.replication = Some(replication);
        self
    }

    pub fn rebind(
        &mut self,
        factory: Arc<dyn SourceFactory>,
        checkpoint: &ScopeCheckpoint,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            checkpoint.system_id == self.checkpoint.system_id
                && checkpoint.database_oid == self.checkpoint.database_oid
                && checkpoint.timeline == self.checkpoint.timeline
                && checkpoint.captured == self.checkpoint.captured,
            "source session checkpoint differs"
        );
        self.factory = factory;
        Ok(())
    }

    pub fn captured(&self) -> Option<u64> {
        self.checkpoint.captured
    }

    pub async fn feedback(&mut self, durable: u64) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.checkpoint.captured == Some(durable),
            "feedback exceeds captured source cut"
        );
        if self.streaming {
            self.replication
                .as_mut()
                .context("replication connection absent")?
                .feedback(durable)
                .await?;
        }
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.sql.is_none() {
            self.sql = Some(self.factory.connect(ConnectionKind::Query).await?);
        }
        if self.connected {
            return Ok(());
        }
        let mut replication = match self.replication.take() {
            Some(replication) => replication,
            None => self.factory.connect(ConnectionKind::Replication).await?,
        };
        catalog::lock_source(
            &mut replication,
            self.config.source_lock,
            std::time::Duration::from_secs(2),
        )
        .await?;
        let sql = self.sql.as_mut().context("source SQL connection absent")?;
        let identity = catalog::identity(sql).await?;
        anyhow::ensure!(
            identity.system_id == self.checkpoint.system_id
                && identity.database_oid == self.checkpoint.database_oid,
            super::CaptureRefusal::ContinuityLost
        );
        anyhow::ensure!(
            catalog::primary_timeline(&mut replication).await? == self.checkpoint.timeline,
            super::CaptureRefusal::ContinuityLost
        );
        self.replication = Some(replication);
        self.connected = true;
        Ok(())
    }

    async fn require_schema(&mut self) -> anyhow::Result<()> {
        let sql = self.sql.as_mut().context("source SQL connection absent")?;
        let catalog =
            catalog::read(sql, &self.config.publication, self.config.relations.len(), 1600).await?;
        anyhow::ensure!(
            catalog.complete_publication
                && catalog.source_helpers
                && catalog.tables.len() == self.config.relations.len()
                && self
                    .config
                    .relations
                    .iter()
                    .all(|relation| catalog.tables.iter().any(|table| &table.relation == relation)),
            super::CaptureRefusal::SchemaChanged
        );
        Ok(())
    }

    pub async fn capture_one<D: CaptureDestination>(
        &mut self,
        destination: &mut D,
        barrier: &str,
    ) -> anyhow::Result<u64> {
        destination
            .authorize()
            .await
            .map_err(|error| error.context(super::AuthorizationFailure))?;
        let result = self.capture_inner(destination, barrier).await;
        if result.is_err() {
            // The authoritative receipt remains with the destination. A later
            // turn reconstructs the same generation from that receipt.
            self.replication = None;
            self.sql = None;
            self.decoder = None;
            self.streaming = false;
            self.connected = false;
        }
        result
    }

    async fn capture_inner<D: CaptureDestination>(
        &mut self,
        destination: &mut D,
        barrier: &str,
    ) -> anyhow::Result<u64> {
        self.connect().await?;
        self.require_schema().await?;
        if self.checkpoint.captured.is_none() {
            let through = self.bootstrap(destination).await?;
            destination
                .authorize_feedback(through)
                .await
                .map_err(|error| error.context(super::AuthorizationFailure))?;
            let replication = self.replication.as_mut().context("replication connection absent")?;
            replication
                .start_replication(&self.config.slot, &self.config.publication, through)
                .await?;
            self.decoder = Some(Decoder::new(
                self.config.publication.clone(),
                self.config.relations.clone(),
                self.config.transaction_bytes,
            )?);
            self.streaming = true;
            if replication.feedback(through).await.is_err() {
                self.replication = None;
                self.decoder = None;
                self.streaming = false;
                self.connected = false;
            }
            return Ok(through);
        }
        let start = self.checkpoint.captured.context("captured source cut absent")?;
        let sql = self.sql.as_mut().context("source SQL connection absent")?;
        let replication = self.replication.as_mut().context("replication connection absent")?;
        if !self.streaming {
            let slot = catalog::slot_state(sql, &self.config.slot)
                .await?
                .context(super::CaptureRefusal::ContinuityLost)?;
            anyhow::ensure!(slot.is_available, super::CaptureRefusal::ContinuityLost);
            anyhow::ensure!(slot.confirmed <= start, super::CaptureRefusal::SourceAhead);
            replication
                .start_replication(&self.config.slot, &self.config.publication, start)
                .await?;
            self.decoder = Some(Decoder::new(
                self.config.publication.clone(),
                self.config.relations.clone(),
                self.config.transaction_bytes,
            )?);
            self.streaming = true;
        }
        let decoder = self.decoder.as_mut().context("scope decoder absent")?;
        // This is the same source stream and guarded protocol decoder as the
        // ordinary apply worker. Scope mode assembles a complete transaction.
        let authority: &D = &*destination;
        let unit =
            super::capture(replication, sql, decoder, start, barrier, || authority.authorize())
                .await?;
        destination
            .authorize()
            .await
            .map_err(|error| error.context(super::AuthorizationFailure))?;
        let unit = match unit {
            CaptureUnit::Transaction(transaction) => Unit::Transaction { from: start, transaction },
            CaptureUnit::Progress(through) => Unit::Progress { from: start, through },
        };
        let through = unit.through()?;
        anyhow::ensure!(through > start, "scope capture did not advance");
        let receipt = destination.seal(unit).await?;
        anyhow::ensure!(receipt == through, "destination receipt names another source cut");
        self.checkpoint.captured = Some(through);
        destination
            .authorize_feedback(through)
            .await
            .map_err(|error| error.context(super::AuthorizationFailure))?;
        // A lost ACK does not undo a committed receipt. The host will resume
        // from its durable frontier and the existing slot on the next turn.
        if replication.feedback(through).await.is_err() {
            self.replication = None;
            self.decoder = None;
            self.streaming = false;
            self.connected = false;
        }
        Ok(through)
    }

    async fn bootstrap<D: CaptureDestination>(
        &mut self,
        destination: &mut D,
    ) -> anyhow::Result<u64> {
        let sql = self.sql.as_mut().context("source SQL connection absent")?;
        // An unsealed baseline has acknowledged no input. Replacing only its
        // owned provisional slot cannot discard a captured transaction.
        catalog::drop_slot(sql, &self.config.slot).await?;
        let mut readers = Vec::with_capacity(self.config.snapshot_readers);
        for _ in 0..self.config.snapshot_readers {
            destination
                .authorize()
                .await
                .map_err(|error| error.context(super::AuthorizationFailure))?;
            let mut reader = self.factory.connect(ConnectionKind::Query).await?;
            let identity = catalog::identity(&mut reader).await?;
            anyhow::ensure!(
                identity.system_id == self.checkpoint.system_id
                    && identity.database_oid == self.checkpoint.database_oid,
                super::CaptureRefusal::ContinuityLost
            );
            readers.push(reader);
        }
        let through = snapshot::begin(
            self.replication.as_mut().context("replication connection absent")?,
            sql,
            &mut readers.iter_mut().collect::<Vec<_>>(),
            &self.config.slot,
            &self.config.relations,
        )
        .await?;
        let plan = snapshot::freeze(
            &mut readers[0],
            &self.config.relations,
            self.config.partitions_per_relation,
        )
        .await?;
        self.require_schema().await?;
        let mut cursors = readers
            .into_iter()
            .map(|reader| snapshot::Cursor::new(reader, self.config.fetch_rows))
            .collect::<anyhow::Result<Vec<_>>>()?;
        for (partition, selection) in plan.iter().enumerate() {
            let relation = self
                .config
                .relations
                .iter()
                .find(|r| r.oid == selection.relation)
                .context("scope relation absent")?;
            let cursor = &mut cursors[partition % self.config.snapshot_readers];
            cursor.select(relation, &selection.selection).await?;
            let mut rows = Vec::new();
            let mut bytes = 0usize;
            while let Some(values) = cursor.next().await? {
                let cells: Vec<_> =
                    values.into_iter().map(|value| value.map_or(Cell::Null, Cell::Text)).collect();
                let size = serde_json::to_vec(&cells)?.len();
                anyhow::ensure!(size <= self.config.page_bytes, super::CaptureRefusal::Capacity);
                if !rows.is_empty()
                    && (bytes + size > self.config.page_bytes
                        || rows.len() >= self.config.fetch_rows)
                {
                    destination
                        .authorize()
                        .await
                        .map_err(|error| error.context(super::AuthorizationFailure))?;
                    destination.snapshot_page(partition, std::mem::take(&mut rows), false).await?;
                    bytes = 0;
                }
                bytes += size;
                rows.push(SnapshotRow { relation: relation.oid, cells });
            }
            destination
                .authorize()
                .await
                .map_err(|error| error.context(super::AuthorizationFailure))?;
            destination.snapshot_page(partition, rows, true).await?;
        }
        let seals =
            futures::future::try_join_all(cursors.iter_mut().map(snapshot::Cursor::finish)).await?;
        anyhow::ensure!(
            snapshot::seal_scope(&plan, &seals)? == through,
            "scope snapshot cut differs"
        );
        destination
            .authorize()
            .await
            .map_err(|error| error.context(super::AuthorizationFailure))?;
        anyhow::ensure!(
            destination.seal(Unit::Snapshot { through, plan }).await? == through,
            "snapshot receipt names another cut"
        );
        self.checkpoint.captured = Some(through);
        Ok(through)
    }
}
