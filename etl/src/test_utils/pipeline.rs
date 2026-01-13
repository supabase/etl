use crate::destination::Destination;
use crate::destination::memory::MemoryDestination;
use crate::pipeline::Pipeline;
use crate::state::table::TableReplicationPhaseType;
use crate::store::cleanup::CleanupStore;
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::test_utils::database::{spawn_source_database, test_table_name};
use crate::test_utils::notifying_store::NotifyingStore;
use crate::test_utils::test_destination_wrapper::TestDestinationWrapper;
use crate::types::PipelineId;
use etl_config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig, TableSyncCopyConfig};
use etl_postgres::tokio::test_utils::PgDatabase;
use etl_postgres::types::{TableId, TableName};
use rand::random;
use tokio_postgres::Client;
use uuid::Uuid;

/// Generates a test-specific replication slot name with a random component.
///
/// This function prefixes the provided slot name with "test_" to avoid conflicts
/// with other replication slots and other tests running in parallel.
pub fn test_slot_name(slot_name: &str) -> String {
    let uuid = Uuid::new_v4().simple().to_string();
    format!("test_{slot_name}_{uuid}")
}

/// Builder for creating test pipelines with configurable options.
///
/// This builder provides a fluent interface for constructing `Pipeline` instances
/// with custom configurations. All configuration options have sensible defaults,
/// allowing you to only specify the options you need to customize.
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
///     .with_batch_config(BatchConfig { max_size: 100, max_fill_ms: 5000 })
///     .with_retry_config(2000, 10)
///     .build();
/// ```
pub struct PipelineBuilder<S, D> {
    pg_connection_config: PgConnectionConfig,
    pipeline_id: PipelineId,
    publication_name: String,
    store: S,
    destination: D,
    /// Batch configuration. Defaults to max_size=1, max_fill_ms=1000 if not specified.
    batch: Option<BatchConfig>,
    /// Delay in milliseconds before retrying a failed table operation. Default: 1000ms.
    table_error_retry_delay_ms: u64,
    /// Maximum number of retry attempts for table operations. Default: 2.
    table_error_retry_max_attempts: u32,
    /// Maximum number of concurrent table sync workers. Default: 1.
    max_table_sync_workers: u16,
    /// Table sync copy configuration. Uses default if not specified.
    table_sync_copy: Option<TableSyncCopyConfig>,
}

impl<S, D> PipelineBuilder<S, D>
where
    S: StateStore + SchemaStore + CleanupStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    /// Creates a new pipeline builder with required parameters and default settings.
    ///
    /// # Arguments
    ///
    /// * `pg_connection_config` - PostgreSQL connection configuration
    /// * `pipeline_id` - Unique identifier for the pipeline
    /// * `publication_name` - Name of the PostgreSQL publication to replicate from
    /// * `store` - Store implementation for state, schema, and cleanup operations
    /// * `destination` - Destination for replicated data
    ///
    /// # Default Settings
    ///
    /// * Batch: max_size=1, max_fill_ms=1000
    /// * Retry delay: 1000ms
    /// * Max retry attempts: 2
    /// * Max table sync workers: 1
    /// * Table sync copy: default configuration
    pub fn new(
        pg_connection_config: PgConnectionConfig,
        pipeline_id: PipelineId,
        publication_name: String,
        store: S,
        destination: D,
    ) -> Self {
        Self {
            pg_connection_config,
            pipeline_id,
            publication_name,
            store,
            destination,
            batch: None,
            table_error_retry_delay_ms: 1000,
            table_error_retry_max_attempts: 2,
            max_table_sync_workers: 1,
            table_sync_copy: None,
        }
    }

    /// Sets custom batch configuration.
    ///
    /// # Arguments
    ///
    /// * `batch` - Configuration controlling batch size and timing for processing events
    pub fn with_batch_config(mut self, batch: BatchConfig) -> Self {
        self.batch = Some(batch);
        self
    }

    /// Sets custom table sync copy configuration.
    ///
    /// # Arguments
    ///
    /// * `table_sync_copy` - Configuration for how table syncs are performed
    pub fn with_table_sync_copy_config(mut self, table_sync_copy: TableSyncCopyConfig) -> Self {
        self.table_sync_copy = Some(table_sync_copy);
        self
    }

    /// Sets custom retry configuration for table operations.
    ///
    /// # Arguments
    ///
    /// * `delay_ms` - Delay in milliseconds before retrying a failed operation
    /// * `max_attempts` - Maximum number of retry attempts before giving up
    pub fn with_retry_config(mut self, delay_ms: u64, max_attempts: u32) -> Self {
        self.table_error_retry_delay_ms = delay_ms;
        self.table_error_retry_max_attempts = max_attempts;
        self
    }

    /// Sets the maximum number of concurrent table sync workers.
    ///
    /// # Arguments
    ///
    /// * `workers` - Number of workers to use for parallel table synchronization
    pub fn with_max_table_sync_workers(mut self, workers: u16) -> Self {
        self.max_table_sync_workers = workers;
        self
    }

    /// Builds and returns the configured pipeline.
    ///
    /// This method consumes the builder and creates a `Pipeline` instance with
    /// all the configured settings. Any options not explicitly set will use their
    /// default values.
    pub fn build(self) -> Pipeline<S, D> {
        let config = PipelineConfig {
            id: self.pipeline_id,
            publication_name: self.publication_name,
            pg_connection: self.pg_connection_config,
            batch: self.batch.unwrap_or(BatchConfig {
                max_size: 1,
                max_fill_ms: 1000,
            }),
            table_error_retry_delay_ms: self.table_error_retry_delay_ms,
            table_error_retry_max_attempts: self.table_error_retry_max_attempts,
            max_table_sync_workers: self.max_table_sync_workers,
            table_sync_copy: self.table_sync_copy.unwrap_or_default(),
        };

        Pipeline::new(config, self.store, self.destination)
    }
}

/// Creates a pipeline with default test configuration.
///
/// This is a convenience wrapper around `PipelineBuilder` that creates a pipeline
/// with standard test defaults: small batch size (1), short timeouts (1000ms),
/// and minimal retry attempts (2).
pub fn create_pipeline<S, D>(
    pg_connection_config: &PgConnectionConfig,
    pipeline_id: PipelineId,
    publication_name: String,
    store: S,
    destination: D,
) -> Pipeline<S, D>
where
    S: StateStore + SchemaStore + CleanupStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
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
    pipeline_id: PipelineId,
    publication_name: String,
    store: S,
    destination: D,
    batch: BatchConfig,
) -> Pipeline<S, D>
where
    S: StateStore + SchemaStore + CleanupStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
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
    pipeline_id: PipelineId,
    publication_name: String,
    store: S,
    destination: D,
    table_sync_copy: TableSyncCopyConfig,
) -> Pipeline<S, D>
where
    S: StateStore + SchemaStore + CleanupStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
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

pub async fn create_database_and_pipeline_with_table(
    table_suffix: &str,
    columns: &[(&str, &str)],
) -> (
    PgDatabase<Client>,
    TableName,
    TableId,
    NotifyingStore,
    TestDestinationWrapper<MemoryDestination>,
    Pipeline<NotifyingStore, TestDestinationWrapper<MemoryDestination>>,
    PipelineId,
    String,
) {
    let database = spawn_source_database().await;

    let table_name = test_table_name(table_suffix);
    let table_id = database
        .create_table(table_name.clone(), true, columns)
        .await
        .unwrap();

    let publication_name = format!("pub_{}", random::<u32>());
    database
        .create_publication(&publication_name, &[table_name.clone()])
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        store.clone(),
        destination.clone(),
    );

    // We wait for ready so that we have the apply worker dealing with events, this is the common
    // testing condition which ensures that the table is ready to be streamed from the main apply worker.
    //
    // The rationale for wanting to test ETL mainly on the apply worker is that it's really hard to test
    // ETL in a state before `Ready` since the system will advance on its own. To properly test all
    // the table sync worker states, we would need a way to programmatically drive execution, but we deemed
    // it too much work compared to the benefit it brings.
    let ready = store
        .notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready)
        .await;

    pipeline.start().await.unwrap();

    ready.notified().await;

    (
        database,
        table_name,
        table_id,
        store,
        destination,
        pipeline,
        pipeline_id,
        publication_name,
    )
}
