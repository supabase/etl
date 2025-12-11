use rand::random;
use tokio_postgres::Client;
use etl_config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig};
use uuid::Uuid;
use etl_postgres::tokio::test_utils::PgDatabase;
use etl_postgres::types::{TableId, TableName};
use crate::destination::Destination;
use crate::destination::memory::MemoryDestination;
use crate::pipeline::Pipeline;
use crate::state::table::TableReplicationPhaseType;
use crate::store::cleanup::CleanupStore;
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::test_utils::database::{spawn_source_database, test_table_name};
use crate::test_utils::notify::NotifyingStore;
use crate::test_utils::test_destination_wrapper::TestDestinationWrapper;
use crate::types::PipelineId;

/// Generates a test-specific replication slot name with a random component.
///
/// This function prefixes the provided slot name with "test_" to avoid conflicts
/// with other replication slots and other tests running in parallel.
pub fn test_slot_name(slot_name: &str) -> String {
    let uuid = Uuid::new_v4().simple().to_string();
    format!("test_{slot_name}_{uuid}")
}

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
    let config = PipelineConfig {
        id: pipeline_id,
        publication_name,
        pg_connection: pg_connection_config.clone(),
        batch: BatchConfig {
            max_size: 1,
            max_fill_ms: 1000,
        },
        table_error_retry_delay_ms: 1000,
        table_error_retry_max_attempts: 2,
        max_table_sync_workers: 1,
    };

    Pipeline::new(config, store, destination)
}

pub fn create_pipeline_with<S, D>(
    pg_connection_config: &PgConnectionConfig,
    pipeline_id: PipelineId,
    publication_name: String,
    store: S,
    destination: D,
    batch_config: Option<BatchConfig>,
) -> Pipeline<S, D>
where
    S: StateStore + SchemaStore + CleanupStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let batch = batch_config.unwrap_or(BatchConfig {
        max_size: 1,
        max_fill_ms: 1000,
    });

    let config = PipelineConfig {
        id: pipeline_id,
        publication_name,
        pg_connection: pg_connection_config.clone(),
        batch,
        table_error_retry_delay_ms: 1000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 1,
    };

    Pipeline::new(config, store, destination)
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

    // We wait for sync done so that we have the apply worker dealing with events, this is the common
    // testing condition which ensures that the table is ready to be streamed from the main apply worker.
    //
    // The rationale for wanting to test ETL mainly on the apply worker is that it's really hard to test
    // ETL in a state before `SyncDone` since the system will advance on its own. To properly test all
    // the table sync worker states, we would need a way to programmatically drive execution, but we deemed
    // it too much work compared to the benefit it brings.
    let sync_done = store
        .notify_on_table_state_type(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    sync_done.notified().await;

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