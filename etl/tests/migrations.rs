use etl::{
    config::{
        BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
        PipelineConfig, TableSyncCopyConfig, TlsConfig,
    },
    migrations::run_source_migrations,
    pipeline::Pipeline,
    store::{MemoryStore, PostgresStore},
    test_utils::memory_destination::MemoryDestination,
};
use etl_postgres::tokio::test_utils::PgDatabase;
use etl_telemetry::tracing::init_test_tracing;
use tokio_postgres::Client;
use uuid::Uuid;

const DEFAULT_DATABASE_HOST: &str = "localhost";
const DEFAULT_DATABASE_PORT: &str = "5430";
const DEFAULT_DATABASE_USERNAME: &str = "postgres";
const DEFAULT_DATABASE_PASSWORD: &str = "postgres";

const POSTGRES_STORE_BASE_VERSION: i64 = 20250827000000;
const POSTGRES_STORE_SCHEMA_VERSION: i64 = 20260415090000;
const SOURCE_SCHEMA_CHANGE_VERSION: i64 = 20260415100000;

fn local_pg_connection_config() -> PgConnectionConfig {
    PgConnectionConfig {
        host: std::env::var("TESTS_DATABASE_HOST").unwrap_or(DEFAULT_DATABASE_HOST.into()),
        port: std::env::var("TESTS_DATABASE_PORT")
            .unwrap_or(DEFAULT_DATABASE_PORT.into())
            .parse()
            .expect("TESTS_DATABASE_PORT must be a valid port number"),
        name: Uuid::new_v4().to_string(),
        username: std::env::var("TESTS_DATABASE_USERNAME")
            .unwrap_or(DEFAULT_DATABASE_USERNAME.into()),
        password: std::env::var("TESTS_DATABASE_PASSWORD")
            .ok()
            .or(Some(DEFAULT_DATABASE_PASSWORD.into()))
            .map(Into::into),
        tls: TlsConfig { trusted_root_certs: String::new(), enabled: false },
        keepalive: Default::default(),
    }
}

async fn spawn_unmigrated_database() -> PgDatabase<Client> {
    PgDatabase::new(local_pg_connection_config()).await
}

async fn query_bool(database: &PgDatabase<Client>, query: &str) -> bool {
    let client = database.client.as_ref().expect("database client should be initialized");
    let row = client.query_one(query, &[]).await.unwrap();

    row.get(0)
}

async fn source_helper_exists(database: &PgDatabase<Client>) -> bool {
    query_bool(
        database,
        "select exists (
            select 1
            from pg_catalog.pg_proc p
            join pg_catalog.pg_namespace n on n.oid = p.pronamespace
            where n.nspname = 'etl'
              and p.proname = 'describe_table_schema'
        )",
    )
    .await
}

async fn postgres_store_table_exists(database: &PgDatabase<Client>) -> bool {
    query_bool(database, "select to_regclass('etl.replication_state') is not null").await
}

async fn applied_migration_versions(database: &PgDatabase<Client>) -> Vec<i64> {
    let client = database.client.as_ref().expect("database client should be initialized");
    let rows = client
        .query("select version from etl._sqlx_migrations order by version", &[])
        .await
        .unwrap();

    rows.into_iter().map(|row| row.get(0)).collect()
}

fn pipeline_config(pg_connection: PgConnectionConfig) -> PipelineConfig {
    PipelineConfig {
        id: 1,
        publication_name: "missing_publication".to_owned(),
        pg_connection,
        batch: BatchConfig {
            max_fill_ms: 5000,
            memory_budget_ratio: BatchConfig::DEFAULT_MEMORY_BUDGET_RATIO,
        },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
        max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
        memory_refresh_interval_ms: 100,
        memory_backpressure: Some(MemoryBackpressureConfig::default()),
        table_sync_copy: TableSyncCopyConfig::default(),
        invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_start_runs_source_migrations_without_postgres_store_tables() {
    init_test_tracing();

    let database = spawn_unmigrated_database().await;
    assert!(!source_helper_exists(&database).await);
    assert!(!postgres_store_table_exists(&database).await);

    let store = MemoryStore::new();
    let destination = MemoryDestination::new(store.clone());
    let mut pipeline = Pipeline::new(pipeline_config(database.config.clone()), store, destination);

    let result = pipeline.start().await;
    assert!(result.is_err());

    assert!(source_helper_exists(&database).await);
    assert!(!postgres_store_table_exists(&database).await);
    assert_eq!(applied_migration_versions(&database).await, vec![SOURCE_SCHEMA_CHANGE_VERSION]);
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_store_startup_runs_only_postgres_store_migrations() {
    init_test_tracing();

    let database = spawn_unmigrated_database().await;
    assert!(!source_helper_exists(&database).await);
    assert!(!postgres_store_table_exists(&database).await);

    let _store = PostgresStore::new(1, database.config.clone()).await.unwrap();

    assert!(!source_helper_exists(&database).await);
    assert!(postgres_store_table_exists(&database).await);
    assert_eq!(
        applied_migration_versions(&database).await,
        vec![POSTGRES_STORE_BASE_VERSION, POSTGRES_STORE_SCHEMA_VERSION]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn source_then_postgres_store_migrations_can_share_sqlx_history() {
    init_test_tracing();

    let database = spawn_unmigrated_database().await;

    run_source_migrations(&database.config).await.unwrap();
    assert_eq!(applied_migration_versions(&database).await, vec![SOURCE_SCHEMA_CHANGE_VERSION]);

    let _store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    run_source_migrations(&database.config).await.unwrap();

    assert!(source_helper_exists(&database).await);
    assert!(postgres_store_table_exists(&database).await);
    assert_eq!(
        applied_migration_versions(&database).await,
        vec![
            POSTGRES_STORE_BASE_VERSION,
            POSTGRES_STORE_SCHEMA_VERSION,
            SOURCE_SCHEMA_CHANGE_VERSION,
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_store_then_source_migrations_can_share_sqlx_history() {
    init_test_tracing();

    let database = spawn_unmigrated_database().await;

    let _store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    run_source_migrations(&database.config).await.unwrap();
    let _store = PostgresStore::new(1, database.config.clone()).await.unwrap();

    assert!(source_helper_exists(&database).await);
    assert!(postgres_store_table_exists(&database).await);
    assert_eq!(
        applied_migration_versions(&database).await,
        vec![
            POSTGRES_STORE_BASE_VERSION,
            POSTGRES_STORE_SCHEMA_VERSION,
            SOURCE_SCHEMA_CHANGE_VERSION,
        ]
    );
}
