use std::{
    num::{NonZeroU16, NonZeroU64},
    time::Duration,
};

use etl_config::shared::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
    PipelineConfig, ReplicationSlotConfig, ReplicationSlotPersistence, TableSyncCopyConfig,
};
use etl_postgres::tokio::test_utils::PgDatabase;
use rand::{
    distr::{Alphanumeric, SampleString},
    random,
};
use tokio_postgres::Client;
use uuid::Uuid;

use crate::{
    destination::PipelineDestination,
    error::EtlError,
    pipeline::{Pipeline, PipelineId},
    schema::{TableId, TableName},
    store::PipelineStore,
    test_utils::{
        database::{spawn_source_database, test_table_name},
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        test_destination_wrapper::TestDestinationWrapper,
    },
};

/// Maximum time for an expected terminal failure to finish the pipeline.
const PIPELINE_FAILURE_TIMEOUT: Duration = Duration::from_secs(30);

/// Waits for a pipeline to fail without requesting shutdown.
///
/// Use after arranging a terminal worker failure. Requesting shutdown instead
/// can cancel initialization before the worker observes the failure. Table
/// errors that leave the pipeline running must be observed through the store.
///
/// # Panics
///
/// Panics if the pipeline succeeds or does not finish within 30 seconds.
pub async fn wait_for_pipeline_error<S, D>(pipeline: &Pipeline<S, D>) -> EtlError
where
    S: PipelineStore,
    D: PipelineDestination,
{
    tokio::time::timeout(PIPELINE_FAILURE_TIMEOUT, pipeline.wait())
        .await
        .expect("Pipeline did not finish within the terminal failure timeout")
        .unwrap_err()
}

/// Generates a test-specific replication slot name with a random component.
///
/// This function prefixes the provided slot name with "test_" to avoid
/// conflicts with other replication slots and other tests running in parallel.
pub fn test_slot_name(slot_name: &str) -> String {
    let uuid = Uuid::new_v4().simple().to_string();
    format!("test_{slot_name}_{uuid}")
}

/// Generates a random lowercase alphanumeric pipeline ID for tests.
pub fn random_pipeline_id() -> String {
    Alphanumeric.sample_string(&mut rand::rng(), 25).to_ascii_lowercase()
}

/// Builder for creating test pipelines with configurable options.
///
/// This builder provides a fluent interface for constructing `Pipeline`
/// instances with custom configurations. All configuration options have
/// sensible defaults, allowing you to only specify the options you need to
/// customize.
///
/// # Examples
///
/// ```ignore
/// // Create a pipeline with default settings
/// let pipeline = PipelineBuilder::new(pg_config, id, pub_name, store, dest)
///     .build();
///
/// // Create a pipeline with custom batch and retry configurations
/// let pipeline = PipelineBuilder::new(pg_config, id, pub_name, store, dest)
///     .with_batch_config(BatchConfig {
///         max_fill_ms: 5000,
///         memory_budget_ratio: 0.2,
///         max_bytes: BatchConfig::DEFAULT_MAX_BYTES,
///     })
///     .with_retry_config(2000, 10)
///     .build();
/// ```
pub struct PipelineBuilder<S, D> {
    /// Complete pipeline configuration with test-specific defaults.
    config: PipelineConfig,
    store: S,
    destination: D,
}

impl<S, D> PipelineBuilder<S, D>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    /// Creates a new pipeline builder with required parameters and default
    /// settings.
    ///
    /// # Arguments
    ///
    /// * `pg_connection_config` - PostgreSQL connection configuration
    /// * `pipeline_id` - Unique identifier for the pipeline
    /// * `publication_name` - Name of the PostgreSQL publication to replicate
    ///   from
    /// * `store` - Store implementation for pipeline runtime operations
    /// * `destination` - Destination for replicated data
    ///
    /// # Default Settings
    ///
    /// * Batch: max_fill_ms=1000
    /// * Retry delay: 1000ms
    /// * Max retry attempts: 2
    /// * Max table sync workers: 1
    /// * Table sync copy: default configuration
    pub fn new(
        pg_connection_config: PgConnectionConfig,
        pipeline_id: impl Into<PipelineId>,
        publication_name: String,
        store: S,
        destination: D,
    ) -> Self {
        let pipeline_id: PipelineId = pipeline_id.into();

        let config = PipelineConfig {
            id: pipeline_id.into(),
            publication_name,
            pg_connection: pg_connection_config,
            store_pg_connection: None,
            batch: BatchConfig {
                max_fill_ms: 1000,
                memory_budget_ratio: 0.2,
                max_bytes: BatchConfig::DEFAULT_MAX_BYTES,
            },
            table_error_retry_delay_ms: 1000,
            table_error_retry_max_attempts: NonZeroU16::new(2).unwrap(),
            max_table_sync_workers: NonZeroU16::MIN,
            table_sync_copy: TableSyncCopyConfig::IncludeAllTables,
            invalidated_slot_behavior: InvalidatedSlotBehavior::Error,
            max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
            memory_refresh_interval_ms: PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS,
            table_sync_monitor_refresh_interval_ms:
                PipelineConfig::DEFAULT_TABLE_SYNC_MONITOR_REFRESH_INTERVAL_MS,
            replication_lag_refresh_interval_ms:
                PipelineConfig::DEFAULT_REPLICATION_LAG_REFRESH_INTERVAL_MS,
            settle_durable_interval_ms: None,
            memory_backpressure: Some(MemoryBackpressureConfig {
                activate_threshold: 0.95,
                resume_threshold: 0.85,
            }),
            replication_slot: ReplicationSlotConfig::default(),
            run_source_migrations: true,
            drain_on_shutdown: PipelineConfig::DEFAULT_DRAIN_ON_SHUTDOWN,
            shutdown_drain_timeout_ms: PipelineConfig::DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS,
        };

        Self { config, store, destination }
    }

    /// Sets whether shutdown drains buffered apply work before the apply loop
    /// exits, or abandons it for replay on the next start.
    pub fn with_drain_on_shutdown(mut self, drain_on_shutdown: bool) -> Self {
        self.config.drain_on_shutdown = drain_on_shutdown;
        self
    }

    /// Sets the maximum time workers may drain after shutdown.
    pub fn with_shutdown_drain_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.config.shutdown_drain_timeout_ms = timeout_ms;
        self
    }

    /// Sets whether PostgreSQL failover support is enabled for replication
    /// slots.
    pub fn with_replication_slot_failover(mut self, enabled: bool) -> Self {
        self.config.replication_slot.failover = enabled;
        self
    }

    /// Sets apply-slot persistence. Table-sync slots are always temporary.
    pub fn with_replication_slot_persistence(
        mut self,
        persistence: ReplicationSlotPersistence,
    ) -> Self {
        self.config.replication_slot.persistence = persistence;
        self
    }

    /// Sets custom batch configuration.
    pub fn with_batch_config(mut self, batch: BatchConfig) -> Self {
        self.config.batch = batch;
        self
    }

    /// Sets custom table sync copy configuration.
    pub fn with_table_sync_copy_config(mut self, table_sync_copy: TableSyncCopyConfig) -> Self {
        self.config.table_sync_copy = table_sync_copy;
        self
    }

    /// Sets custom retry configuration for table operations.
    ///
    /// # Panics
    ///
    /// Panics if `max_attempts` is zero.
    pub fn with_retry_config(mut self, delay_ms: u64, max_attempts: u16) -> Self {
        self.config.table_error_retry_delay_ms = delay_ms;
        self.config.table_error_retry_max_attempts = NonZeroU16::new(max_attempts).unwrap();
        self
    }

    /// Sets the maximum number of concurrent table sync workers.
    ///
    /// # Panics
    ///
    /// Panics if `workers` is zero.
    pub fn with_max_table_sync_workers(mut self, workers: u16) -> Self {
        self.config.max_table_sync_workers = NonZeroU16::new(workers).unwrap();
        self
    }

    /// Sets the behavior when the main replication slot is found to be
    /// invalidated.
    pub fn with_invalidated_slot_behavior(mut self, behavior: InvalidatedSlotBehavior) -> Self {
        self.config.invalidated_slot_behavior = behavior;
        self
    }

    /// Sets the maximum number of parallel connections per table during initial
    /// copy.
    ///
    /// # Panics
    ///
    /// Panics if `connections` is zero.
    pub fn with_max_copy_connections_per_table(mut self, connections: u16) -> Self {
        self.config.max_copy_connections_per_table = NonZeroU16::new(connections).unwrap();
        self
    }

    /// Sets the milliseconds between copy-slot validity checks.
    ///
    /// # Panics
    ///
    /// Panics if `interval_ms` is zero.
    pub fn with_table_sync_monitor_refresh_interval_ms(mut self, interval_ms: u64) -> Self {
        self.config.table_sync_monitor_refresh_interval_ms = NonZeroU64::new(interval_ms).unwrap();
        self
    }

    /// Sets the milliseconds between pipeline-wide lag samples.
    ///
    /// # Panics
    ///
    /// Panics if `interval_ms` is zero.
    pub fn with_replication_lag_refresh_interval_ms(mut self, interval_ms: u64) -> Self {
        self.config.replication_lag_refresh_interval_ms = NonZeroU64::new(interval_ms).unwrap();
        self
    }

    /// Sets the minimum age of an accepted write before an idle keepalive may
    /// force durability.
    ///
    /// `None` allows forced durability at any keepalive. `Some` guarantees at
    /// least that window before a later keepalive can force durability.
    pub fn with_settle_durable_interval_ms(mut self, interval_ms: Option<u64>) -> Self {
        self.config.settle_durable_interval_ms = interval_ms;
        self
    }

    /// Builds and returns the configured pipeline.
    ///
    /// This method consumes the builder and creates a `Pipeline` instance with
    /// all the configured settings. Any options not explicitly set will use
    /// their default values.
    pub fn build(self) -> Pipeline<S, D> {
        Pipeline::new(self.config, self.store, self.destination)
    }
}

/// Creates a pipeline with default test configuration.
///
/// This is a convenience wrapper around `PipelineBuilder` that creates a
/// pipeline with standard test defaults: small batch size (1), short timeouts
/// (1000ms), and minimal retry attempts (2).
pub fn create_pipeline<S, D>(
    pg_connection_config: &PgConnectionConfig,
    pipeline_id: impl Into<PipelineId>,
    publication_name: String,
    store: S,
    destination: D,
) -> Pipeline<S, D>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    PipelineBuilder::new(
        pg_connection_config.clone(),
        pipeline_id,
        publication_name,
        store,
        destination,
    )
    .build()
}

/// Creates a pipeline with custom batch configuration.
///
/// This variant allows customizing the batch processing behavior while using
/// default values for other settings. Note that this also increases the maximum
/// retry attempts to 5 (vs the default 2).
pub fn create_pipeline_with_batch_config<S, D>(
    pg_connection_config: &PgConnectionConfig,
    pipeline_id: impl Into<PipelineId>,
    publication_name: String,
    store: S,
    destination: D,
    batch: BatchConfig,
) -> Pipeline<S, D>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    PipelineBuilder::new(
        pg_connection_config.clone(),
        pipeline_id,
        publication_name,
        store,
        destination,
    )
    .with_batch_config(batch)
    .with_retry_config(1000, 5)
    .build()
}

/// Creates a pipeline with custom table sync copy configuration.
///
/// This variant allows customizing how table synchronization is performed while
/// using default values for other settings.
pub fn create_pipeline_with_table_sync_copy_config<S, D>(
    pg_connection_config: &PgConnectionConfig,
    pipeline_id: impl Into<PipelineId>,
    publication_name: String,
    store: S,
    destination: D,
    table_sync_copy: TableSyncCopyConfig,
) -> Pipeline<S, D>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    PipelineBuilder::new(
        pg_connection_config.clone(),
        pipeline_id,
        publication_name,
        store,
        destination,
    )
    .with_table_sync_copy_config(table_sync_copy)
    .build()
}

/// Creates an empty published table and waits for its pipeline to reach
/// [`crate::replication::state::TableStateType::SyncDone`].
pub async fn create_database_and_sync_done_pipeline_with_table(
    table_suffix: &str,
    columns: &[(&str, &str)],
) -> (
    PgDatabase<Client>,
    TableName,
    TableId,
    NotifyingStore,
    TestDestinationWrapper<MemoryDestination<NotifyingStore>>,
    Pipeline<NotifyingStore, TestDestinationWrapper<MemoryDestination<NotifyingStore>>>,
    PipelineId,
    String,
) {
    let database = spawn_source_database().await;

    let table_name = test_table_name(table_suffix);
    let table_id = database.create_table(table_name.clone(), true, columns).await.unwrap();

    let publication_name = format!("pub_{}", random::<u32>());
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random_pipeline_id().into();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id.clone(),
        publication_name.clone(),
        store.clone(),
        destination.clone(),
    );

    let table_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;

    (database, table_name, table_id, store, destination, pipeline, pipeline_id, publication_name)
}
