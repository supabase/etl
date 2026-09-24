use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, LazyLock},
    time::Duration,
};

use etl_postgres::store::{
    checkpoint, destination_table_metadata as pg_destination_table_metadata, schema,
    table_state as pg_table_state,
};
use metrics::{counter, gauge};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::sync::{Mutex, MutexGuard};
use tokio_postgres::types::PgLsn;
use tracing::{debug, info};

use crate::{
    config::{IntoConnectOptions, PgConnectionConfig, PgConnectionOptions},
    destination::{DestinationTableMetadata, DestinationTableSchema},
    error::{ErrorKind, EtlResult},
    etl_error,
    observability::{
        ETL_SCHEMA_CLEANUP_SKIPPED_TABLES_TOTAL, ETL_TABLES_TOTAL, POSTGRES_STORE_POOL, STATE_LABEL,
    },
    pipeline::PipelineId,
    postgres::{
        migrations,
        pool::{InstrumentedPgPool, InstrumentedPgTransaction},
    },
    replication::{
        WorkerType,
        state::{TableState, TableStateType},
    },
    schema::{ReplicationMask, SnapshotId, TableId, TableSchema},
    store::{
        CachedStore, DestinationTablesMetadata, SchemaStore, StateStore, TableSchemaSnapshots,
        TableStateLifecycleStore, TableStateOperation, TableStates,
    },
};

/// Maximum number of connections in the pool.
const MAX_POOL_CONNECTIONS: u32 = 2;

/// Duration after which idle connections are closed.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Application name for Postgres state store connections.
const APP_NAME_REPLICATOR_STORE: &str = "supabase_etl_replicator_store";

/// Stable prefix for pipeline transaction locks, including during recovery.
const STORE_LOCK_NAMESPACE: &str = "supabase_etl_store:";

/// Connection options for Postgres state store queries.
///
/// These settings intentionally mirror out-of-band query timeouts while using a
/// store-specific application name for clearer database observability.
static POSTGRES_STORE_OPTIONS: LazyLock<PgConnectionOptions> =
    LazyLock::new(|| PgConnectionOptions::builder(APP_NAME_REPLICATOR_STORE).build());

/// Creates a lazily connected pool with automatic idle connection cleanup.
fn create_database_pool(connection_config: &PgConnectionConfig) -> InstrumentedPgPool {
    // Reload queries need fresh database snapshots after acquiring the
    // transaction lock, even if the database defaults to repeatable read.
    let options: PgConnectOptions = connection_config.with_db(Some(&POSTGRES_STORE_OPTIONS));
    let options = options.options([("default_transaction_isolation", "read committed")]);

    let pool = PgPoolOptions::new()
        .min_connections(0)
        .max_connections(MAX_POOL_CONNECTIONS)
        .idle_timeout(Some(IDLE_TIMEOUT))
        .connect_lazy_with(options);

    InstrumentedPgPool::new(pool, POSTGRES_STORE_POOL)
}

/// Emits table-related metrics which quantify the total number of tables in
/// each state.
fn emit_table_metrics(counts_by_state: &HashMap<TableStateType, u64>) {
    for (state, count) in counts_by_state {
        let label: &'static str = (*state).into();
        gauge!(ETL_TABLES_TOTAL, STATE_LABEL => label).set(*count as f64);
    }
}

/// Inner state of [`PostgresStore`].
///
/// The shared mutex orders cache access and database mutations. Lifecycle
/// operations keep every affected cache in the same critical section.
#[derive(Debug, Default)]
struct Inner {
    /// False until a complete load or mutation publishes confirmed state.
    ///
    /// Cancellation drops futures without executing their error paths. Mark
    /// the cache unusable before database work and mark it usable only after
    /// a full reload or a confirmed mutation while holding the mutex.
    usable: bool,
    /// Number of tables in each state, used for metrics.
    state_counts: HashMap<TableStateType, u64>,
    /// Cached table states indexed by table ID.
    table_states: TableStates,
    /// Eagerly loaded checkpoints shared by every clone of this store.
    replication_checkpoints: HashMap<WorkerType, PgLsn>,
    /// All retained schema versions, kept until durable-checkpoint pruning.
    table_schemas: Arc<TableSchemaSnapshots>,
    /// Last successfully pruned retention boundary per table.
    ///
    /// Schema writes at or below a remembered boundary and table lifecycle
    /// changes clear the affected entries. Full reloads clear every entry.
    schema_prune_boundaries: BTreeMap<TableId, SnapshotId>,
    /// Cached destination table metadata indexed by table ID.
    destination_tables_metadata: DestinationTablesMetadata,
}

impl Inner {
    /// Initializes state counts from an existing table state map.
    fn init_state_counts(&mut self, table_states: &BTreeMap<TableId, TableState>) {
        let mut state_counts = HashMap::new();
        for state in table_states.values() {
            let state_type: TableStateType = state.into();
            *state_counts.entry(state_type).or_insert(0u64) += 1;
        }
        self.state_counts = state_counts;
    }

    /// Inserts or updates a table state and adjusts state counts accordingly.
    fn set_table_state(&mut self, table_id: TableId, state: TableState) {
        let states = Arc::make_mut(&mut self.table_states);

        // Decrement old state count if the state existed.
        if let Some(old_state) = states.get(&table_id) {
            let old_state_type: TableStateType = old_state.into();
            if let Some(count) = self.state_counts.get_mut(&old_state_type) {
                *count = count.saturating_sub(1);
            }
        }

        // Increment new state count.
        let new_state: TableStateType = (&state).into();
        let state_count = self.state_counts.entry(new_state).or_default();
        *state_count = state_count.saturating_add(1);

        states.insert(table_id, state);
    }

    /// Removes a table state and adjusts state counts accordingly.
    fn remove_table_state(&mut self, table_id: TableId) {
        let states = Arc::make_mut(&mut self.table_states);
        if let Some(old_state) = states.remove(&table_id) {
            let old_state_type: TableStateType = (&old_state).into();
            if let Some(count) = self.state_counts.get_mut(&old_state_type) {
                *count = count.saturating_sub(1);
            }
        }
    }
}

/// Postgres-backed storage for ETL pipeline state and schema information.
///
/// Implements [`crate::store::PipelineStore`] with shared caches and a lazy
/// connection pool. Follows the ownership and caching contract in
/// [`crate::store`].
///
/// # Concurrency and recovery
///
/// Use one active store and its clones per pipeline; independent cached owners
/// and external metadata writes are unsupported. One mutex covers cache reads,
/// database mutations, and publication because lifecycle operations span
/// several cache domains. Returned values remain snapshots, and separate getter
/// calls do not form one transaction.
///
/// Mutations mark the cache unusable before database awaits: cancellation skips
/// error handling and may leave COMMIT running after the mutex is released.
/// Every mutation and reload takes the same pipeline-scoped transaction
/// advisory lock, so recovery waits for that database outcome, even on a new
/// connection. The lock orders database work; it does not synchronize
/// independent caches.
///
/// READ COMMITTED gives data queries fresh snapshots after the separate lock
/// statement finishes. REPEATABLE READ could retain a snapshot from before the
/// wait. Holding the lock throughout loading excludes participating writers
/// between queries, keeping all cache domains consistent.
///
/// Cache updates follow confirmed COMMIT without another await. New stores and
/// interrupted operations remain unusable until an accessor reloads every
/// domain and publishes a complete replacement. Failed reloads return errors
/// and remain eligible for retry. Reloading clears pruning memoization because
/// an uncertain schema commit may have invalidated a remembered boundary.
#[derive(Debug, Clone)]
pub struct PostgresStore {
    pipeline_id: PipelineId,
    pool: InstrumentedPgPool,
    /// One serialization point for cache access and database mutations.
    inner: Arc<Mutex<Inner>>,
}

impl PostgresStore {
    /// Creates a new Postgres-backed store for the given pipeline.
    ///
    /// Runs the Postgres store migrations, then creates a lazily-connected pool
    /// with automatic idle timeout. Connections are established on first use
    /// and automatically closed after `IDLE_TIMEOUT` of inactivity.
    ///
    /// Install the metrics recorder before constructing the store so its
    /// cached query metric handle uses that recorder.
    /// The cache starts unusable and is loaded by [`CachedStore::load_cache`]
    /// or the first accessor.
    pub async fn new(
        pipeline_id: PipelineId,
        connection_config: PgConnectionConfig,
    ) -> EtlResult<Self> {
        migrations::run_postgres_store_migrations(&connection_config).await?;

        let pool = create_database_pool(&connection_config);

        Ok(Self { pipeline_id, pool, inner: Arc::new(Mutex::new(Inner::default())) })
    }

    /// Returns a usable cache, rebuilding every domain after interruption.
    async fn lock_cache(&self) -> EtlResult<MutexGuard<'_, Inner>> {
        let mut inner = self.inner.lock().await;
        if !inner.usable {
            self.refresh_cache(&mut inner).await?;
        }

        Ok(inner)
    }

    /// Starts a transaction ordered after every earlier store mutation.
    ///
    /// Acquire the lock before issuing any mutation. Dropping a future can
    /// leave its statement or COMMIT executing on another pooled connection;
    /// PostgreSQL holds this lock until that transaction commits or rolls back.
    /// Read committed ensures queries after the wait see its final outcome.
    async fn begin_transaction(&self) -> EtlResult<InstrumentedPgTransaction> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("select pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("{STORE_LOCK_NAMESPACE}{}", self.pipeline_id))
            .execute(tx.executor())
            .await?;

        Ok(tx)
    }

    /// Atomically replaces every cache after observing settled database state.
    ///
    /// Keep the previous data inaccessible until all queries and conversions
    /// succeed. Failed or cancelled reloads remain eligible for a full retry.
    async fn refresh_cache(&self, inner: &mut Inner) -> EtlResult<()> {
        inner.usable = false;
        let mut tx = self.begin_transaction().await?;

        let mut refreshed = Inner::default();

        let replication_state_rows =
            pg_table_state::get_table_state_rows(tx.executor(), self.pipeline_id as i64).await?;

        let mut table_states: BTreeMap<TableId, TableState> = BTreeMap::new();
        for row in replication_state_rows {
            let table_id = TableId::new(row.table_id.0);
            let state = TableState::from_state_row(row)?;
            table_states.insert(table_id, state);
        }

        let rows = pg_destination_table_metadata::load_destination_tables_metadata(
            tx.executor(),
            self.pipeline_id as i64,
        )
        .await
        .map_err(|err| {
            etl_error!(
                ErrorKind::SourceQueryFailed,
                "Destination tables metadata loading failed",
                source: err
            )
        })?;

        let mut metadata: BTreeMap<TableId, DestinationTableMetadata> = BTreeMap::new();
        for (table_id, row) in rows {
            let target_mask = ReplicationMask::from_bytes(row.replication_mask);
            let destination_metadata = match (
                row.schema_status,
                row.previous_snapshot_id,
                row.previous_replication_mask,
            ) {
                (
                    pg_destination_table_metadata::StoredDestinationTableSchemaStatus::Creating,
                    None,
                    None,
                ) => DestinationTableMetadata::new_creating(
                    row.destination_table_id,
                    row.snapshot_id,
                    target_mask,
                ),
                (
                    pg_destination_table_metadata::StoredDestinationTableSchemaStatus::Applying,
                    Some(previous_snapshot_id),
                    Some(previous_replication_mask),
                ) => DestinationTableMetadata::new_applied(
                    row.destination_table_id,
                    previous_snapshot_id,
                    ReplicationMask::from_bytes(previous_replication_mask),
                )
                .with_schema_change(row.snapshot_id, target_mask)?,
                (
                    pg_destination_table_metadata::StoredDestinationTableSchemaStatus::Applied,
                    None,
                    None,
                ) => DestinationTableMetadata::new_applied(
                    row.destination_table_id,
                    row.snapshot_id,
                    target_mask,
                ),
                (schema_status, previous_snapshot_id, previous_replication_mask) => {
                    return Err(etl_error!(
                        ErrorKind::InvalidState,
                        "Destination table metadata has an invalid schema endpoint",
                        format!(
                            "table '{}' has schema status '{schema_status:?}', previous snapshot \
                             present: {}, previous replication mask present: {}",
                            row.destination_table_id,
                            previous_snapshot_id.is_some(),
                            previous_replication_mask.is_some()
                        )
                    ));
                }
            };
            metadata.insert(table_id, destination_metadata);
        }

        let table_schemas = schema::load_table_schemas(tx.executor(), self.pipeline_id as i64)
            .await
            .map_err(|error| {
                etl_error!(
                    ErrorKind::SourceQueryFailed,
                    "Table schemas loading failed",
                    source: error
                )
            })?;

        let checkpoints =
            checkpoint::load_replication_checkpoints(tx.executor(), self.pipeline_id as i64)
                .await
                .map_err(|err| {
                    etl_error!(
                        ErrorKind::SourceQueryFailed,
                        "Replication checkpoints loading failed",
                        source: err
                    )
                })?;

        refreshed.replication_checkpoints = checkpoints
            .into_iter()
            .map(|(table_id, lsn)| {
                let worker = table_id
                    .map_or(WorkerType::Apply, |table_id| WorkerType::TableSync { table_id });

                (worker, lsn)
            })
            .collect();

        refreshed.init_state_counts(&table_states);
        refreshed.table_states = Arc::new(table_states);
        refreshed.destination_tables_metadata = Arc::new(metadata);
        Arc::make_mut(&mut refreshed.table_schemas).replace_all(table_schemas);

        tx.commit().await?;

        // A reload can remove the last table in a previously emitted state.
        for state in inner.state_counts.keys() {
            refreshed.state_counts.entry(*state).or_default();
        }
        refreshed.usable = true;
        *inner = refreshed;
        emit_table_metrics(&inner.state_counts);
        info!("loaded postgres store caches");

        Ok(())
    }
}

impl CachedStore for PostgresStore {
    async fn load_cache(&self) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        self.refresh_cache(&mut inner).await
    }
}

impl StateStore for PostgresStore {
    /// Returns a cached table state, refreshing every cache if unusable.
    async fn get_table_state(&self, table_id: TableId) -> EtlResult<Option<TableState>> {
        let inner = self.lock_cache().await?;

        Ok(inner.table_states.get(&table_id).cloned())
    }

    /// Returns a complete snapshot of cached table states.
    async fn get_table_states(&self) -> EtlResult<TableStates> {
        let inner = self.lock_cache().await?;

        Ok(Arc::clone(&inner.table_states))
    }

    /// Commits all table-state updates, then updates the shared cache.
    async fn update_table_states(&self, updates: Vec<(TableId, TableState)>) -> EtlResult<()> {
        let mut inner = self.lock_cache().await?;
        if updates.is_empty() {
            return Ok(());
        }

        // Convert all states upfront to catch any conversion errors
        // before starting the transaction.
        let db_updates: Vec<(TableId, pg_table_state::StoredTableStateType, serde_json::Value)> =
            updates
                .iter()
                .map(|(table_id, state)| {
                    let (state_type, metadata) = state.to_storage_format()?;

                    Ok((*table_id, state_type, metadata))
                })
                .collect::<EtlResult<Vec<_>>>()?;

        // Perform all database updates in a single transaction.
        inner.usable = false;
        let mut tx = self.begin_transaction().await?;

        for (table_id, state_type, metadata) in db_updates {
            pg_table_state::update_table_state_raw(
                tx.executor(),
                self.pipeline_id as i64,
                table_id,
                state_type,
                metadata,
            )
            .await?;
        }

        tx.commit().await?;

        // Update the cache.
        for (table_id, state) in updates {
            inner.set_table_state(table_id, state);
        }
        emit_table_metrics(&inner.state_counts);
        inner.usable = true;

        Ok(())
    }

    /// Rolls back a table to its previous state.
    ///
    /// Returns the restored state, or an error if no previous state exists.
    async fn rollback_table_state(&self, table_id: TableId) -> EtlResult<TableState> {
        let mut inner = self.lock_cache().await?;

        inner.usable = false;
        let mut tx = self.begin_transaction().await?;

        let restored_row =
            pg_table_state::rollback_table_state(tx.executor(), self.pipeline_id as i64, table_id)
                .await?
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::StateRollbackError,
                        "Previous table state not found",
                        "No previous state available to roll back to for this table"
                    )
                })?;

        let restored_state = TableState::from_state_row(restored_row)?;

        tx.commit().await?;

        inner.set_table_state(table_id, restored_state.clone());
        emit_table_metrics(&inner.state_counts);
        inner.usable = true;

        Ok(restored_state)
    }

    async fn get_replication_checkpoint(
        &self,
        worker_type: WorkerType,
    ) -> EtlResult<Option<PgLsn>> {
        let inner = self.lock_cache().await?;

        Ok(inner.replication_checkpoints.get(&worker_type).copied())
    }

    async fn upsert_replication_checkpoint(
        &self,
        worker_type: WorkerType,
        checkpoint_lsn: PgLsn,
    ) -> EtlResult<PgLsn> {
        let mut inner = self.lock_cache().await?;

        inner.usable = false;
        let mut tx = self.begin_transaction().await?;

        let persisted_checkpoint = checkpoint::upsert_replication_checkpoint(
            tx.executor(),
            self.pipeline_id as i64,
            worker_type.as_str(),
            worker_type.checkpoint_table_id(),
            checkpoint_lsn,
        )
        .await
        .map_err(|err| {
            etl_error!(
                ErrorKind::SourceQueryFailed,
                "Replication checkpoint storage failed",
                source: err
            )
        })?;

        tx.commit().await?;

        inner.replication_checkpoints.insert(worker_type, persisted_checkpoint);
        inner.usable = true;

        Ok(persisted_checkpoint)
    }

    async fn delete_replication_checkpoint(&self, worker_type: WorkerType) -> EtlResult<()> {
        let mut inner = self.lock_cache().await?;

        inner.usable = false;
        let mut tx = self.begin_transaction().await?;

        checkpoint::delete_replication_checkpoint(
            tx.executor(),
            self.pipeline_id as i64,
            worker_type.as_str(),
            worker_type.checkpoint_table_id(),
        )
        .await
        .map_err(|err| {
            etl_error!(
                ErrorKind::SourceQueryFailed,
                "Replication checkpoint deletion failed",
                source: err
            )
        })?;

        tx.commit().await?;

        inner.replication_checkpoints.remove(&worker_type);
        inner.usable = true;

        Ok(())
    }

    /// Returns cached destination metadata, refreshing every cache if unusable.
    async fn get_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<DestinationTableMetadata>> {
        let inner = self.lock_cache().await?;

        Ok(inner.destination_tables_metadata.get(&table_id).cloned())
    }

    /// Stores complete destination table metadata in both database and cache.
    async fn store_destination_table_metadata(
        &self,
        table_id: TableId,
        metadata: DestinationTableMetadata,
    ) -> EtlResult<()> {
        let mut inner = self.lock_cache().await?;
        debug!(
            %table_id,
            destination_table_id = %metadata.table_id(),
            "storing destination table metadata"
        );

        let (previous_snapshot_id, previous_replication_mask, schema_status) =
            match metadata.table_schema() {
                DestinationTableSchema::Creating { .. } => (
                    None,
                    None,
                    pg_destination_table_metadata::StoredDestinationTableSchemaStatus::Creating,
                ),
                DestinationTableSchema::Applying {
                    previous_snapshot_id,
                    previous_replication_mask,
                    ..
                } => (
                    Some(*previous_snapshot_id),
                    Some(previous_replication_mask.as_slice()),
                    pg_destination_table_metadata::StoredDestinationTableSchemaStatus::Applying,
                ),
                DestinationTableSchema::Applied { .. } => (
                    None,
                    None,
                    pg_destination_table_metadata::StoredDestinationTableSchemaStatus::Applied,
                ),
            };

        inner.usable = false;
        let mut tx = self.begin_transaction().await?;

        pg_destination_table_metadata::store_destination_table_metadata(
            tx.executor(),
            self.pipeline_id as i64,
            table_id,
            metadata.table_id(),
            metadata.snapshot_id(),
            metadata.replication_mask().as_slice(),
            previous_snapshot_id,
            previous_replication_mask,
            schema_status,
        )
        .await
        .map_err(|err| {
            etl_error!(
                ErrorKind::SourceQueryFailed,
                "Destination table metadata storage failed",
                source: err
            )
        })?;

        tx.commit().await?;

        Arc::make_mut(&mut inner.destination_tables_metadata).insert(table_id, metadata);
        inner.usable = true;

        Ok(())
    }
}

impl SchemaStore for PostgresStore {
    /// Returns the newest retained schema at or before the requested snapshot.
    ///
    /// Refreshes every cache first if it is uninitialized or unusable.
    async fn get_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        let inner = self.lock_cache().await?;

        Ok(inner.table_schemas.get_at_or_before(*table_id, snapshot_id))
    }

    /// Returns all retained schema versions, refreshing every cache if
    /// unusable.
    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let inner = self.lock_cache().await?;

        Ok(inner.table_schemas.all())
    }

    /// Stores a table schema in both database and cache.
    ///
    /// Commits the schema replacement before updating the shared cache. The
    /// schema's `snapshot_id` determines which version this schema represents.
    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<Arc<TableSchema>> {
        let mut inner = self.lock_cache().await?;
        debug!(
            table_name = %table_schema.name,
            snapshot_id = %table_schema.snapshot_id,
            "storing table schema"
        );

        inner.usable = false;
        let mut tx = self.begin_transaction().await?;

        let schema_id =
            schema::upsert_table_schema(tx.executor(), self.pipeline_id as i64, &table_schema)
                .await?;
        schema::delete_table_columns(tx.executor(), schema_id).await?;
        for columns in table_schema.column_schemas.chunks(schema::MAX_COLUMNS_PER_INSERT) {
            schema::insert_table_columns(tx.executor(), schema_id, columns).await?;
        }

        tx.commit().await?;

        if inner
            .schema_prune_boundaries
            .get(&table_schema.id)
            .is_some_and(|boundary| table_schema.snapshot_id <= *boundary)
        {
            inner.schema_prune_boundaries.remove(&table_schema.id);
        }

        let schema = Arc::make_mut(&mut inner.table_schemas).insert(table_schema);
        inner.usable = true;

        Ok(schema)
    }

    async fn prune_table_schemas(
        &self,
        mut retention_snapshot_ids: BTreeMap<TableId, SnapshotId>,
    ) -> EtlResult<u64> {
        let mut inner = self.lock_cache().await?;

        let requested_table_count = retention_snapshot_ids.len();
        retention_snapshot_ids.retain(|table_id, boundary| {
            inner.schema_prune_boundaries.get(table_id).is_none_or(|pruned| *pruned < *boundary)
        });
        let skipped_table_count = requested_table_count - retention_snapshot_ids.len();
        if skipped_table_count > 0 {
            counter!(ETL_SCHEMA_CLEANUP_SKIPPED_TABLES_TOTAL).increment(skipped_table_count as u64);
        }
        if retention_snapshot_ids.is_empty() {
            return Ok(0);
        }

        inner.usable = false;
        let mut tx = self.begin_transaction().await?;

        let deleted_count = schema::delete_obsolete_table_schema_versions(
            tx.executor(),
            self.pipeline_id as i64,
            &retention_snapshot_ids,
        )
        .await
        .map_err(|err| {
            etl_error!(
                ErrorKind::SourceQueryFailed,
                "Obsolete table schema deletion failed",
                source: err
            )
        })?;

        tx.commit().await?;

        let cached_count = Arc::make_mut(&mut inner.table_schemas).prune(&retention_snapshot_ids);

        if deleted_count > 0 || cached_count > 0 {
            debug!(
                deleted_count,
                cached_count,
                table_count = retention_snapshot_ids.len(),
                "pruned obsolete table schema versions from postgres state store"
            );
        }

        // Only confirmed success establishes a completed boundary,
        // including queries that delete no versions.
        inner.schema_prune_boundaries.extend(retention_snapshot_ids);
        inner.usable = true;

        Ok(deleted_count)
    }
}

impl TableStateLifecycleStore for PostgresStore {
    async fn apply_table_state_operation(&self, operation: TableStateOperation) -> EtlResult<()> {
        let mut inner = self.lock_cache().await?;

        match operation {
            TableStateOperation::PrepareForCopy { table_id } => {
                let worker_type = WorkerType::TableSync { table_id };
                inner.usable = false;
                let mut tx = self.begin_transaction().await?;

                pg_destination_table_metadata::delete_destination_table_metadata(
                    tx.executor(),
                    self.pipeline_id as i64,
                    table_id,
                )
                .await
                .map_err(|err| {
                    etl_error!(
                        ErrorKind::SourceQueryFailed,
                        "Destination table metadata deletion failed",
                        source: err
                    )
                })?;

                schema::delete_table_schema_for_table(
                    tx.executor(),
                    self.pipeline_id as i64,
                    table_id,
                )
                .await
                .map_err(|err| {
                    etl_error!(
                        ErrorKind::SourceQueryFailed,
                        "Table schema deletion failed",
                        source: err
                    )
                })?;

                checkpoint::delete_replication_checkpoint_for_table(
                    tx.executor(),
                    self.pipeline_id as i64,
                    table_id,
                )
                .await
                .map_err(|err| {
                    etl_error!(
                        ErrorKind::SourceQueryFailed,
                        "Replication checkpoint deletion failed",
                        source: err
                    )
                })?;

                tx.commit().await?;

                inner.schema_prune_boundaries.remove(&table_id);
                inner.replication_checkpoints.remove(&worker_type);

                Arc::make_mut(&mut inner.table_schemas).remove_table(table_id);
                Arc::make_mut(&mut inner.destination_tables_metadata).remove(&table_id);
                inner.usable = true;

                Ok(())
            }
            TableStateOperation::ResetForResync => {
                let (state_type, metadata) = TableState::Init.to_storage_format()?;

                let table_ids = inner.table_states.keys().copied().collect::<Vec<_>>();

                inner.usable = false;
                let mut tx = self.begin_transaction().await?;

                for table_id in &table_ids {
                    pg_table_state::update_table_state_raw(
                        tx.executor(),
                        self.pipeline_id as i64,
                        *table_id,
                        state_type,
                        metadata.clone(),
                    )
                    .await?;
                }

                checkpoint::delete_replication_checkpoint(
                    tx.executor(),
                    self.pipeline_id as i64,
                    WorkerType::Apply.as_str(),
                    WorkerType::Apply.checkpoint_table_id(),
                )
                .await
                .map_err(|err| {
                    etl_error!(
                        ErrorKind::SourceQueryFailed,
                        "Replication checkpoint deletion failed",
                        source: err
                    )
                })?;

                tx.commit().await?;

                inner.schema_prune_boundaries.clear();
                inner.replication_checkpoints.remove(&WorkerType::Apply);

                for table_id in table_ids {
                    inner.set_table_state(table_id, TableState::Init);
                }
                emit_table_metrics(&inner.state_counts);
                inner.usable = true;

                Ok(())
            }
            TableStateOperation::Delete { table_id } => {
                let worker_type = WorkerType::TableSync { table_id };
                inner.usable = false;
                let mut tx = self.begin_transaction().await?;

                pg_destination_table_metadata::delete_destination_table_metadata(
                    tx.executor(),
                    self.pipeline_id as i64,
                    table_id,
                )
                .await
                .map_err(|err| {
                    etl_error!(
                        ErrorKind::SourceQueryFailed,
                        "Destination table metadata deletion failed",
                        source: err
                    )
                })?;

                schema::delete_table_schema_for_table(
                    tx.executor(),
                    self.pipeline_id as i64,
                    table_id,
                )
                .await
                .map_err(|err| {
                    etl_error!(
                        ErrorKind::SourceQueryFailed,
                        "Table schema deletion failed",
                        source: err
                    )
                })?;

                pg_table_state::delete_table_state(
                    tx.executor(),
                    self.pipeline_id as i64,
                    table_id,
                )
                .await?;

                checkpoint::delete_replication_checkpoint_for_table(
                    tx.executor(),
                    self.pipeline_id as i64,
                    table_id,
                )
                .await
                .map_err(|err| {
                    etl_error!(
                        ErrorKind::SourceQueryFailed,
                        "Replication checkpoint deletion failed",
                        source: err
                    )
                })?;

                tx.commit().await?;

                inner.schema_prune_boundaries.remove(&table_id);
                inner.replication_checkpoints.remove(&worker_type);

                inner.remove_table_state(table_id);
                emit_table_metrics(&inner.state_counts);
                Arc::make_mut(&mut inner.table_schemas).remove_table(table_id);
                Arc::make_mut(&mut inner.destination_tables_metadata).remove(&table_id);
                inner.usable = true;

                Ok(())
            }
        }
    }
}
