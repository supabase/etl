use std::{collections::BTreeSet, sync::LazyLock, time::Duration};

use etl::{
    config::{
        BatchConfig, IntoConnectOptions, InvalidatedSlotBehavior, MemoryBackpressureConfig,
        PgConnectionConfig, PgConnectionOptions, PipelineConfig, TableSyncCopyConfig,
    },
    pipeline::Pipeline,
    postgres::{
        client::PgReplicationClient,
        migrations::{ETL_SCHEMA_LOCK_ID, run_source_migrations},
    },
    store::{MemoryStore, PostgresStore},
    test_utils::{memory_destination::MemoryDestination, pipeline::test_slot_name},
};
use etl_postgres::{
    test_utils::{local_tls_config_from_env, test_database_port_from_env},
    tokio::test_utils::PgDatabase,
};
use etl_telemetry::tracing::init_test_tracing;
use futures::StreamExt;
use pg_escape::quote_identifier;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use sqlx::{Connection, Executor, PgConnection, migrate::Migrator, postgres::PgConnectOptions};
use tokio_postgres::Client;
use uuid::Uuid;

const DEFAULT_DATABASE_HOST: &str = "localhost";
const DEFAULT_DATABASE_USERNAME: &str = "postgres";
const DEFAULT_DATABASE_PASSWORD: &str = "postgres";
const POSTGRES_STORE_BASE_VERSION: i64 = 20250827000000;
const POSTGRES_STORE_PRE_COMPOSITE_SNAPSHOT_VERSION: i64 = 20260610120000;
const POSTGRES_STORE_PRE_CREATING_STATUS_VERSION: i64 = 20260810120000;
const SOURCE_PRE_PUBLICATION_SCHEMA_CHANGE_VERSION: i64 = 20260415100000;
const SOURCE_PRE_PRIMARY_KEY_DEFERRABILITY_VERSION: i64 = 20260724120000;
const SOURCE_PRE_SYNTHETIC_RELATION_VERSION: i64 = 20261002120000;
const APP_NAME_TEST_MIGRATIONS: &str = "supabase_etl_test_migrations";

static TEST_MIGRATION_OPTIONS: LazyLock<PgConnectionOptions> =
    LazyLock::new(|| PgConnectionOptions::builder(APP_NAME_TEST_MIGRATIONS).build());

fn local_pg_connection_config() -> PgConnectionConfig {
    PgConnectionConfig {
        host: std::env::var("TESTS_DATABASE_HOST").unwrap_or(DEFAULT_DATABASE_HOST.into()),
        hostaddr: None,
        port: test_database_port_from_env(),
        name: Uuid::new_v4().to_string(),
        username: std::env::var("TESTS_DATABASE_USERNAME")
            .unwrap_or(DEFAULT_DATABASE_USERNAME.into()),
        password: std::env::var("TESTS_DATABASE_PASSWORD")
            .ok()
            .or(Some(DEFAULT_DATABASE_PASSWORD.into()))
            .map(Into::into),
        tls: local_tls_config_from_env(),
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

async fn migration_connection(connection_config: &PgConnectionConfig) -> PgConnection {
    let options: PgConnectOptions = connection_config.with_db(Some(&TEST_MIGRATION_OPTIONS));
    let mut conn = PgConnection::connect_with(&options).await.unwrap();

    conn.execute("set client_min_messages = warning;").await.unwrap();
    conn.execute("create schema if not exists etl;").await.unwrap();
    conn.execute("set search_path = 'etl';").await.unwrap();

    conn
}

fn source_migrator() -> Migrator {
    let mut migrator = sqlx::migrate!("./migrations/source");
    migrator.set_ignore_missing(true);
    migrator
}

fn postgres_store_migrator() -> Migrator {
    let mut migrator = sqlx::migrate!("./migrations/postgres_store");
    migrator.set_ignore_missing(true);
    migrator
}

fn up_migration_versions(migrator: Migrator) -> Vec<i64> {
    let mut versions = migrator
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| migration.version)
        .collect::<Vec<_>>();
    versions.sort_unstable();
    versions
}

fn source_migration_versions() -> Vec<i64> {
    up_migration_versions(source_migrator())
}

fn postgres_store_migration_versions() -> Vec<i64> {
    up_migration_versions(postgres_store_migrator())
}

fn all_split_migration_versions() -> Vec<i64> {
    let mut versions = postgres_store_migration_versions();
    versions.extend(source_migration_versions());
    versions.sort_unstable();
    versions
}

fn assert_migration_set_is_reversible(migration_set: &str, migrator: Migrator) {
    let mut up_versions = BTreeSet::new();
    let mut down_versions = BTreeSet::new();

    for migration in migrator.iter() {
        if migration.migration_type.is_up_migration() {
            up_versions.insert(migration.version);
        } else if migration.migration_type.is_down_migration() {
            down_versions.insert(migration.version);
        } else {
            panic!(
                "{migration_set} migration {} is not reversible; use `sqlx migrate add -r`",
                migration.version
            );
        }
    }

    assert_eq!(up_versions, down_versions);
}

fn pipeline_config(pg_connection: PgConnectionConfig) -> PipelineConfig {
    PipelineConfig {
        id: 1,
        publication_name: "missing_publication".to_owned(),
        pg_connection,
        store_pg_connection: None,
        replication_slot: Default::default(),
        batch: BatchConfig {
            max_fill_ms: 5000,
            memory_budget_ratio: 0.2,
            max_bytes: 8 * 1024 * 1024,
        },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
        max_copy_connections_per_table: 2,
        memory_refresh_interval_ms: 100,
        table_sync_monitor_refresh_interval_ms: 10000,
        replication_lag_refresh_interval_ms:
            PipelineConfig::DEFAULT_REPLICATION_LAG_REFRESH_INTERVAL_MS,
        settle_durable_interval_ms: None,
        memory_backpressure: Some(MemoryBackpressureConfig::default()),
        table_sync_copy: TableSyncCopyConfig::default(),
        invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
        run_source_migrations: true,
        drain_on_shutdown: true,
        shutdown_drain_timeout_ms: PipelineConfig::DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS,
    }
}

#[test]
fn split_migration_sets_are_reversible() {
    assert_migration_set_is_reversible("postgres_store", postgres_store_migrator());
    assert_migration_set_is_reversible("source", source_migrator());
}

#[test]
fn split_migration_versions_are_globally_unique() {
    let mut seen = BTreeSet::new();

    for (migration_set, version) in postgres_store_migration_versions()
        .into_iter()
        .map(|version| ("postgres_store", version))
        .chain(source_migration_versions().into_iter().map(|version| ("source", version)))
    {
        assert!(
            seen.insert(version),
            "migration version {version} is duplicated in the {migration_set} migration set"
        );
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
    assert_eq!(applied_migration_versions(&database).await, source_migration_versions());
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
    assert_eq!(applied_migration_versions(&database).await, postgres_store_migration_versions());
}

#[tokio::test(flavor = "multi_thread")]
async fn creating_status_migration_disambiguates_initial_setup() {
    init_test_tracing();

    let database = spawn_unmigrated_database().await;
    let mut conn = migration_connection(&database.config).await;
    postgres_store_migrator()
        .run_direct(Some(POSTGRES_STORE_PRE_CREATING_STATUS_VERSION), &mut conn, false)
        .await
        .unwrap();

    conn.execute(
        "insert into etl.destination_tables_metadata (
            pipeline_id,
            table_id,
            destination_table_id,
            snapshot_id,
            previous_snapshot_id,
            previous_replication_mask,
            schema_status,
            replication_mask
        )
        values
            (1, 41, 'creating_table', '10:11', null, null, 'applying', decode('01', 'hex')),
            (1, 42, 'applying_table', '20:21', '10:11', decode('01', 'hex'), 'applying', \
         decode('0101', 'hex')),
            (1, 43, 'applied_table', '30:31', null, null, 'applied', decode('01', 'hex'))",
    )
    .await
    .unwrap();

    postgres_store_migrator().run_direct(None, &mut conn, false).await.unwrap();

    let statuses: Vec<(String, String)> = sqlx::query_as(
        "select destination_table_id, schema_status::text
         from etl.destination_tables_metadata
         order by destination_table_id",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        statuses,
        vec![
            ("applied_table".to_owned(), "applied".to_owned()),
            ("applying_table".to_owned(), "applying".to_owned()),
            ("creating_table".to_owned(), "creating".to_owned()),
        ]
    );

    postgres_store_migrator()
        .undo(&mut conn, POSTGRES_STORE_PRE_CREATING_STATUS_VERSION)
        .await
        .unwrap();

    let statuses: Vec<String> = sqlx::query_scalar(
        "select schema_status::text
         from etl.destination_tables_metadata
         order by destination_table_id",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(statuses, vec!["applied", "applying", "applying"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn composite_snapshot_migration_preserves_legacy_message_lsn_ordering() {
    init_test_tracing();

    let database = spawn_unmigrated_database().await;
    let mut conn = migration_connection(&database.config).await;
    postgres_store_migrator()
        .run_direct(Some(POSTGRES_STORE_PRE_COMPOSITE_SNAPSHOT_VERSION), &mut conn, false)
        .await
        .unwrap();
    drop(conn);

    let client = database.client.as_ref().expect("database client should be initialized");
    let table_id = 42_u32;
    client
        .execute(
            "insert into etl.table_schemas (
                pipeline_id,
                table_id,
                schema_name,
                table_name,
                snapshot_id
            )
            values ($1, $2, 'public', 'items', '0/2'::pg_catalog.pg_lsn)",
            &[&1_i64, &table_id],
        )
        .await
        .unwrap();
    let above_i64_table_id = 46_u32;
    client
        .execute(
            "insert into etl.table_schemas (
                pipeline_id,
                table_id,
                schema_name,
                table_name,
                snapshot_id
            )
            values (
                $1,
                $2,
                'public',
                'above_i64_snapshot_items',
                '80000000/0'::pg_catalog.pg_lsn
            )",
            &[&1_i64, &above_i64_table_id],
        )
        .await
        .unwrap();
    let initial_table_id = 43_u32;
    client
        .execute(
            "insert into etl.table_schemas (
                pipeline_id,
                table_id,
                schema_name,
                table_name
            )
            values ($1, $2, 'public', 'initial_items')",
            &[&1_i64, &initial_table_id],
        )
        .await
        .unwrap();
    let max_table_id = 45_u32;
    client
        .execute(
            "insert into etl.table_schemas (
                pipeline_id,
                table_id,
                schema_name,
                table_name,
                snapshot_id
            )
            values (
                $1,
                $2,
                'public',
                'max_snapshot_items',
                'FFFFFFFF/FFFFFFFF'::pg_catalog.pg_lsn
            )",
            &[&1_i64, &max_table_id],
        )
        .await
        .unwrap();
    client
        .execute(
            "insert into etl.destination_tables_metadata (
                pipeline_id,
                table_id,
                destination_table_id,
                snapshot_id,
                previous_snapshot_id,
                schema_status,
                replication_mask
            )
            values (
                $1,
                $2,
                'destination_max_snapshot_items',
                'FFFFFFFF/FFFFFFFF'::pg_catalog.pg_lsn,
                '80000000/0'::pg_catalog.pg_lsn,
                'applying',
                decode('01', 'hex')
            )",
            &[&1_i64, &max_table_id],
        )
        .await
        .unwrap();
    client
        .execute(
            "insert into etl.destination_tables_metadata (
                pipeline_id,
                table_id,
                destination_table_id,
                snapshot_id,
                previous_snapshot_id,
                schema_status,
                replication_mask
            )
            values (
                $1,
                $2,
                'destination_items',
                '0/2'::pg_catalog.pg_lsn,
                '0/1'::pg_catalog.pg_lsn,
                'applying',
                decode('01', 'hex')
            )",
            &[&1_i64, &table_id],
        )
        .await
        .unwrap();
    client
        .execute(
            "insert into etl.destination_tables_metadata (
                pipeline_id,
                table_id,
                destination_table_id,
                snapshot_id,
                schema_status,
                replication_mask
            )
            values (
                $1,
                $2,
                'destination_initial_items',
                '0/0'::pg_catalog.pg_lsn,
                'applied',
                decode('01', 'hex')
            )",
            &[&1_i64, &initial_table_id],
        )
        .await
        .unwrap();

    let mut conn = migration_connection(&database.config).await;
    postgres_store_migrator().run_direct(None, &mut conn, false).await.unwrap();
    drop(conn);

    let schema_snapshot_id: String = client
        .query_one(
            "select snapshot_id from etl.table_schemas where pipeline_id = $1 and table_id = $2",
            &[&1_i64, &table_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(schema_snapshot_id, "2:2");
    let initial_schema_snapshot_id: String = client
        .query_one(
            "select snapshot_id from etl.table_schemas where pipeline_id = $1 and table_id = $2",
            &[&1_i64, &initial_table_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(initial_schema_snapshot_id, "0:0");
    let max_schema_snapshot_id: String = client
        .query_one(
            "select snapshot_id from etl.table_schemas where pipeline_id = $1 and table_id = $2",
            &[&1_i64, &max_table_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(max_schema_snapshot_id, "18446744073709551615:18446744073709551615");
    let above_i64_schema_snapshot_id: String = client
        .query_one(
            "select snapshot_id from etl.table_schemas where pipeline_id = $1 and table_id = $2",
            &[&1_i64, &above_i64_table_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(above_i64_schema_snapshot_id, "9223372036854775808:9223372036854775808");

    let metadata_snapshot_ids_row = client
        .query_one(
            "select snapshot_id, previous_snapshot_id
            from etl.destination_tables_metadata
            where pipeline_id = $1 and table_id = $2",
            &[&1_i64, &table_id],
        )
        .await
        .unwrap();
    let metadata_snapshot_ids: (String, Option<String>) =
        (metadata_snapshot_ids_row.get(0), metadata_snapshot_ids_row.get(1));
    assert_eq!(metadata_snapshot_ids, ("2:2".to_owned(), Some("1:1".to_owned()),));
    let initial_metadata_row = client
        .query_one(
            "select snapshot_id, previous_snapshot_id
            from etl.destination_tables_metadata
            where pipeline_id = $1 and table_id = $2",
            &[&1_i64, &initial_table_id],
        )
        .await
        .unwrap();
    let initial_metadata_snapshot_ids: (String, Option<String>) =
        (initial_metadata_row.get(0), initial_metadata_row.get(1));
    assert_eq!(initial_metadata_snapshot_ids, ("0:0".to_owned(), None));
    let max_metadata_row = client
        .query_one(
            "select snapshot_id, previous_snapshot_id
            from etl.destination_tables_metadata
            where pipeline_id = $1 and table_id = $2",
            &[&1_i64, &max_table_id],
        )
        .await
        .unwrap();
    let max_metadata_snapshot_ids: (String, Option<String>) =
        (max_metadata_row.get(0), max_metadata_row.get(1));
    assert_eq!(
        max_metadata_snapshot_ids,
        (
            "18446744073709551615:18446744073709551615".to_owned(),
            Some("9223372036854775808:9223372036854775808".to_owned()),
        )
    );

    let new_initial_table_id = 44_u32;
    let new_initial_snapshot_id: String = client
        .query_one(
            "insert into etl.table_schemas (
                pipeline_id,
                table_id,
                schema_name,
                table_name
            )
            values ($1, $2, 'public', 'new_initial_items')
            returning snapshot_id",
            &[&1_i64, &new_initial_table_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(new_initial_snapshot_id, "0:0");

    let ordered_migrated_snapshot_ids: Vec<String> = client
        .query(
            "select snapshot_id
            from etl.table_schemas
            where pipeline_id = $1 and table_id in ($2, $3, $4, $5)
            order by
                pg_catalog.split_part(snapshot_id, ':', 1)::pg_catalog.numeric,
                pg_catalog.split_part(snapshot_id, ':', 2)::pg_catalog.numeric",
            &[&1_i64, &initial_table_id, &table_id, &above_i64_table_id, &max_table_id],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        ordered_migrated_snapshot_ids,
        vec![
            "0:0".to_owned(),
            "2:2".to_owned(),
            "9223372036854775808:9223372036854775808".to_owned(),
            "18446744073709551615:18446744073709551615".to_owned(),
        ]
    );

    let snapshot_columns: Vec<(String, String, String)> = client
        .query(
            "select table_name, column_name, data_type
            from information_schema.columns
            where table_schema = 'etl'
              and column_name in ('snapshot_id', 'previous_snapshot_id')
              and table_name in ('table_schemas', 'destination_tables_metadata')
            order by table_name, column_name",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    assert_eq!(
        snapshot_columns,
        vec![
            (
                "destination_tables_metadata".to_owned(),
                "previous_snapshot_id".to_owned(),
                "text".to_owned(),
            ),
            ("destination_tables_metadata".to_owned(), "snapshot_id".to_owned(), "text".to_owned(),),
            ("table_schemas".to_owned(), "snapshot_id".to_owned(), "text".to_owned(),),
        ]
    );

    let mut conn = migration_connection(&database.config).await;
    postgres_store_migrator()
        .undo(&mut conn, POSTGRES_STORE_PRE_COMPOSITE_SNAPSHOT_VERSION)
        .await
        .unwrap();
    drop(conn);

    let restored_snapshot_id: String = client
        .query_one(
            "select snapshot_id::pg_catalog.text
            from etl.table_schemas
            where pipeline_id = $1 and table_id = $2",
            &[&1_i64, &table_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(restored_snapshot_id, "0/2");

    let restored_initial_snapshot_id: String = client
        .query_one(
            "select snapshot_id::pg_catalog.text
            from etl.table_schemas
            where pipeline_id = $1 and table_id = $2",
            &[&1_i64, &initial_table_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(restored_initial_snapshot_id, "0/0");

    let restored_max_snapshot_id: String = client
        .query_one(
            "select snapshot_id::pg_catalog.text
            from etl.table_schemas
            where pipeline_id = $1 and table_id = $2",
            &[&1_i64, &max_table_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(restored_max_snapshot_id, "FFFFFFFF/FFFFFFFF");
    let restored_above_i64_snapshot_id: String = client
        .query_one(
            "select snapshot_id::pg_catalog.text
            from etl.table_schemas
            where pipeline_id = $1 and table_id = $2",
            &[&1_i64, &above_i64_table_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(restored_above_i64_snapshot_id, "80000000/0");

    let restored_metadata_row = client
        .query_one(
            "select snapshot_id::pg_catalog.text, previous_snapshot_id::pg_catalog.text
            from etl.destination_tables_metadata
            where pipeline_id = $1 and table_id = $2",
            &[&1_i64, &table_id],
        )
        .await
        .unwrap();
    let restored_metadata_snapshot_ids: (String, Option<String>) =
        (restored_metadata_row.get(0), restored_metadata_row.get(1));
    assert_eq!(restored_metadata_snapshot_ids, ("0/2".to_owned(), Some("0/1".to_owned())));

    let restored_initial_metadata_row = client
        .query_one(
            "select snapshot_id::pg_catalog.text, previous_snapshot_id::pg_catalog.text
            from etl.destination_tables_metadata
            where pipeline_id = $1 and table_id = $2",
            &[&1_i64, &initial_table_id],
        )
        .await
        .unwrap();
    let restored_initial_metadata_snapshot_ids: (String, Option<String>) =
        (restored_initial_metadata_row.get(0), restored_initial_metadata_row.get(1));
    assert_eq!(restored_initial_metadata_snapshot_ids, ("0/0".to_owned(), None));
    let restored_max_metadata_row = client
        .query_one(
            "select snapshot_id::pg_catalog.text, previous_snapshot_id::pg_catalog.text
            from etl.destination_tables_metadata
            where pipeline_id = $1 and table_id = $2",
            &[&1_i64, &max_table_id],
        )
        .await
        .unwrap();
    let restored_max_metadata_snapshot_ids: (String, Option<String>) =
        (restored_max_metadata_row.get(0), restored_max_metadata_row.get(1));
    assert_eq!(
        restored_max_metadata_snapshot_ids,
        ("FFFFFFFF/FFFFFFFF".to_owned(), Some("80000000/0".to_owned()),)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn source_then_postgres_store_migrations_can_share_sqlx_history() {
    init_test_tracing();

    let database = spawn_unmigrated_database().await;

    run_source_migrations(&database.config).await.unwrap();
    assert_eq!(applied_migration_versions(&database).await, source_migration_versions());

    let _store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    run_source_migrations(&database.config).await.unwrap();

    assert!(source_helper_exists(&database).await);
    assert!(postgres_store_table_exists(&database).await);
    assert_eq!(applied_migration_versions(&database).await, all_split_migration_versions());
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
    assert_eq!(applied_migration_versions(&database).await, all_split_migration_versions());
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_store_schema_storage_migration_round_trips_old_state() {
    init_test_tracing();

    let database = spawn_unmigrated_database().await;

    let mut conn = migration_connection(&database.config).await;
    postgres_store_migrator().run_direct(None, &mut conn, false).await.unwrap();
    postgres_store_migrator().undo(&mut conn, POSTGRES_STORE_BASE_VERSION).await.unwrap();
    drop(conn);

    let client = database.client.as_ref().expect("database client should be initialized");
    client
        .batch_execute(
            "create table public.migration_round_trip_items (
                id integer not null,
                tenant_id integer not null,
                value text,
                primary key (tenant_id, id)
            );",
        )
        .await
        .unwrap();

    let table_id: u32 = client
        .query_one("select 'public.migration_round_trip_items'::regclass::oid", &[])
        .await
        .unwrap()
        .get(0);
    let schema_id: i64 = client
        .query_one(
            "insert into etl.table_schemas (
                pipeline_id,
                table_id,
                schema_name,
                table_name
            )
            values ($1, $2, $3, $4)
            returning id",
            &[&7_i64, &table_id, &"public", &"migration_round_trip_items"],
        )
        .await
        .unwrap()
        .get(0);

    for (column_name, column_type, nullable, primary_key, column_order) in [
        ("id", "integer", false, true, 0_i32),
        ("tenant_id", "integer", false, true, 1_i32),
        ("value", "text", true, false, 2_i32),
    ] {
        client
            .execute(
                "insert into etl.table_columns (
                    table_schema_id,
                    column_name,
                    column_type,
                    type_modifier,
                    nullable,
                    primary_key,
                    column_order
                )
                values ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    &schema_id,
                    &column_name,
                    &column_type,
                    &(-1_i32),
                    &nullable,
                    &primary_key,
                    &column_order,
                ],
            )
            .await
            .unwrap();
    }

    client
        .execute(
            "insert into etl.table_mappings (
                pipeline_id,
                source_table_id,
                destination_table_id
            )
            values ($1, $2, $3)",
            &[&7_i64, &table_id, &"destination_round_trip_items"],
        )
        .await
        .unwrap();

    let mut conn = migration_connection(&database.config).await;
    postgres_store_migrator().run_direct(None, &mut conn, false).await.unwrap();
    drop(conn);

    let migrated_columns: String = client
        .query_one(
            "select string_agg(
                column_name || ':' ||
                ordinal_position::text || ':' ||
                coalesce(primary_key_ordinal_position::text, 'null'),
                ',' order by column_name
            )
            from etl.table_columns",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(migrated_columns, "id:1:2,tenant_id:2:1,value:3:null");

    let destination_metadata_row = client
        .query_one(
            "select destination_table_id, encode(replication_mask, 'hex')
            from etl.destination_tables_metadata
            where pipeline_id = $1 and table_id = $2",
            &[&7_i64, &table_id],
        )
        .await
        .unwrap();
    let destination_table_id: String = destination_metadata_row.get(0);
    let replication_mask: String = destination_metadata_row.get(1);
    assert_eq!(destination_table_id, "destination_round_trip_items");
    assert_eq!(replication_mask, "010101");
    assert!(!query_bool(&database, "select to_regclass('etl.table_mappings') is not null").await);

    let mut conn = migration_connection(&database.config).await;
    postgres_store_migrator().undo(&mut conn, POSTGRES_STORE_BASE_VERSION).await.unwrap();
    drop(conn);

    assert!(query_bool(&database, "select to_regclass('etl.table_mappings') is not null").await);
    let reverted_columns: String = client
        .query_one(
            "select string_agg(
                column_name || ':' ||
                column_order::text || ':' ||
                primary_key::text,
                ',' order by column_name
            )
            from etl.table_columns",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(reverted_columns, "id:0:true,tenant_id:1:true,value:2:false");

    let restored_mapping: String = client
        .query_one(
            "select destination_table_id
            from etl.table_mappings
            where pipeline_id = $1 and source_table_id = $2",
            &[&7_i64, &table_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(restored_mapping, "destination_round_trip_items");
    assert!(
        !query_bool(
            &database,
            "select exists (
                select 1
                from information_schema.columns
                where table_schema = 'etl'
                  and table_name = 'table_schemas'
                  and column_name = 'snapshot_id'
            )"
        )
        .await
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_store_schema_storage_down_migration_keeps_destination_snapshot() {
    init_test_tracing();

    let database = spawn_unmigrated_database().await;
    let _store = PostgresStore::new(1, database.config.clone()).await.unwrap();

    let client = database.client.as_ref().expect("database client should be initialized");
    client
        .batch_execute(
            "create table public.migration_snapshot_items (
                id integer primary key,
                value text,
                extra text
            );",
        )
        .await
        .unwrap();

    let table_id: u32 = client
        .query_one("select 'public.migration_snapshot_items'::regclass::oid", &[])
        .await
        .unwrap()
        .get(0);

    let first_schema_id: i64 = client
        .query_one(
            "insert into etl.table_schemas (
                pipeline_id,
                table_id,
                schema_name,
                table_name,
                snapshot_id
            )
            values ($1, $2, $3, $4, '0:1')
            returning id",
            &[&11_i64, &table_id, &"public", &"migration_snapshot_items"],
        )
        .await
        .unwrap()
        .get(0);
    let second_schema_id: i64 = client
        .query_one(
            "insert into etl.table_schemas (
                pipeline_id,
                table_id,
                schema_name,
                table_name,
                snapshot_id
            )
            values ($1, $2, $3, $4, '0:2')
            returning id",
            &[&11_i64, &table_id, &"public", &"migration_snapshot_items"],
        )
        .await
        .unwrap()
        .get(0);

    for (schema_id, columns) in [
        (first_schema_id, [("id", 1_i32, Some(1_i32)), ("value", 2_i32, None)]),
        (second_schema_id, [("id", 1_i32, Some(1_i32)), ("extra", 2_i32, None)]),
    ] {
        for (column_name, ordinal_position, primary_key_ordinal_position) in columns {
            client
                .execute(
                    "insert into etl.table_columns (
                        table_schema_id,
                        column_name,
                        column_type,
                        type_modifier,
                        nullable,
                        ordinal_position,
                        primary_key_ordinal_position
                    )
                    values ($1, $2, $3, $4, $5, $6, $7)",
                    &[
                        &schema_id,
                        &column_name,
                        &"text",
                        &(-1_i32),
                        &false,
                        &ordinal_position,
                        &primary_key_ordinal_position,
                    ],
                )
                .await
                .unwrap();
        }
    }

    client
        .execute(
            "insert into etl.destination_tables_metadata (
                pipeline_id,
                table_id,
                destination_table_id,
                snapshot_id,
                previous_snapshot_id,
                schema_status,
                replication_mask
            )
            values (
                $1,
                $2,
                $3,
                '0:2',
                '0:1',
                'applied',
                decode('0101', 'hex')
            )",
            &[&11_i64, &table_id, &"destination_snapshot_items"],
        )
        .await
        .unwrap();

    let mut conn = migration_connection(&database.config).await;
    postgres_store_migrator().undo(&mut conn, POSTGRES_STORE_BASE_VERSION).await.unwrap();
    drop(conn);

    let schema_count: i64 = client
        .query_one(
            "select count(*)
            from etl.table_schemas
            where pipeline_id = $1 and table_id = $2",
            &[&11_i64, &table_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(schema_count, 1);

    let reverted_columns: String = client
        .query_one(
            "select string_agg(
                tc.column_name || ':' ||
                tc.column_order::text || ':' ||
                tc.primary_key::text,
                ',' order by tc.column_name
            )
            from etl.table_schemas ts
            join etl.table_columns tc
              on tc.table_schema_id = ts.id
            where ts.pipeline_id = $1 and ts.table_id = $2",
            &[&11_i64, &table_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(reverted_columns, "extra:1:false,id:0:true");

    let restored_mapping: String = client
        .query_one(
            "select destination_table_id
            from etl.table_mappings
            where pipeline_id = $1 and source_table_id = $2",
            &[&11_i64, &table_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(restored_mapping, "destination_snapshot_items");
}

/// Fresh installations of the previous source migration set can retain WAL
/// through an upgrade, including payloads readable by the previous decoder.
#[tokio::test(flavor = "multi_thread")]
async fn source_upgrade_from_main_preserves_queued_identity_messages() {
    /// The required identity fields read by the pre-synthetic decoder.
    #[derive(serde::Deserialize)]
    struct LegacyIdentity {
        /// Destination primary-key metadata.
        primary_key_attnums: Vec<i32>,
        /// Whether the source primary key is deferrable.
        #[serde(default)]
        primary_key_condeferrable: bool,
        /// The source replica-identity mode.
        relreplident: String,
        /// The explicit-index field required by the previous decoder.
        replica_identity_index_attnums: Vec<i32>,
    }

    let database = spawn_unmigrated_database().await;
    let mut connection = migration_connection(&database.config).await;
    source_migrator().run_to(SOURCE_PRE_SYNTHETIC_RELATION_VERSION, &mut connection).await.unwrap();
    drop(connection);
    assert_eq!(
        applied_migration_versions(&database).await,
        source_migration_versions()
            .into_iter()
            .filter(|version| *version <= SOURCE_PRE_SYNTHETIC_RELATION_VERSION)
            .collect::<Vec<_>>()
    );
    let client = database.client.as_ref().unwrap();
    client
        .batch_execute(
            "create table public.upgrade_identity (id integer primary key, value integer not \
             null);
             create unique index upgrade_identity_value on public.upgrade_identity (value);
             create publication upgrade_pub for table public.upgrade_identity",
        )
        .await
        .unwrap();
    let mut replication_client =
        PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let slot_name = test_slot_name("identity_upgrade");
    let slot = replication_client.create_slot(&slot_name, &Default::default()).await.unwrap();

    client
        .batch_execute(
            "alter table public.upgrade_identity replica identity using index \
             upgrade_identity_value",
        )
        .await
        .unwrap();

    run_source_migrations(&database.config).await.unwrap();

    client
        .batch_execute(
            "alter table public.upgrade_identity owner to current_user;
             alter table public.upgrade_identity replica identity default",
        )
        .await
        .unwrap();

    // Start decoding only after both formats are already queued in the slot.
    let (stream, _) = replication_client
        .start_logical_replication("upgrade_pub", &slot_name, slot.consistent_point, None)
        .await
        .unwrap();
    tokio::pin!(stream);
    let mut messages = Vec::<serde_json::Value>::new();
    while messages.len() < 3 {
        let event = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let ReplicationMessage::XLogData(event) = event
            && let LogicalReplicationMessage::Message(message) = event.data()
            && message.prefix().unwrap() == "supabase_etl_ddl"
        {
            messages.push(serde_json::from_slice(message.content()).unwrap());
        }
    }
    assert!(messages[0].get("publication_column_attnums").is_none());
    assert!(messages[0]["identity"].get("replica_identity_attnums").is_none());
    assert_eq!(messages[1]["identity"]["replica_identity_attnums"], serde_json::json!([2]));
    assert_eq!(messages[2]["identity"]["replica_identity_attnums"], serde_json::json!([1]));
    for (message, mode, legacy_keys) in
        [(&messages[0], "i", vec![2]), (&messages[1], "i", vec![2]), (&messages[2], "d", vec![])]
    {
        let legacy: LegacyIdentity = serde_json::from_value(message["identity"].clone()).unwrap();
        assert_eq!(legacy.primary_key_attnums, vec![1]);
        assert!(!legacy.primary_key_condeferrable);
        assert_eq!(legacy.relreplident, mode);
        assert_eq!(legacy.replica_identity_index_attnums, legacy_keys);
    }
}

/// Column output formats follow the configured SQLx migration connection rather
/// than duplicated defaults in the migration SQL.
#[tokio::test(flavor = "multi_thread")]
async fn source_migration_captures_connection_output_formats() {
    let database = spawn_unmigrated_database().await;
    let mut session_options = (*TEST_MIGRATION_OPTIONS).clone();
    session_options.extra_float_digits = 1;
    let options: PgConnectOptions = database.config.with_db(Some(&session_options));
    let mut conn = PgConnection::connect_with(&options).await.unwrap();
    let application_name: String =
        sqlx::query_scalar("show application_name").fetch_one(&mut conn).await.unwrap();
    assert_eq!(application_name, session_options.application_name);
    conn.execute("create schema etl; set search_path = etl").await.unwrap();

    source_migrator().run(&mut conn).await.unwrap();

    let (count, matches_connection): (i64, bool) = sqlx::query_as(
        "select count(*), bool_and(
             split_part(config, '=', 2) = current_setting(split_part(config, '=', 1))
         )
         from pg_catalog.pg_proc p
         cross join lateral unnest(p.proconfig) config
         where p.oid = 'etl.describe_table_columns(pg_catalog.oid)'::regprocedure
           and lower(split_part(config, '=', 1)) = any(array[
               'datestyle', 'intervalstyle', 'extra_float_digits', 'timezone', 'bytea_output'
           ])",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(count, 5);
    assert!(matches_connection);
    let float_setting: String =
        sqlx::query_scalar("show extra_float_digits").fetch_one(&mut conn).await.unwrap();
    assert_eq!(float_setting, "1");
}

/// The production migration runner applies shared ETL options at connection
/// startup, overriding conflicting database defaults.
#[tokio::test(flavor = "multi_thread")]
async fn source_migrations_use_shared_connection_defaults() {
    let database = spawn_unmigrated_database().await;
    database
        .client
        .as_ref()
        .unwrap()
        .batch_execute(&format!(
            "alter database {} set extra_float_digits = -15",
            quote_identifier(&database.config.name)
        ))
        .await
        .unwrap();

    run_source_migrations(&database.config).await.unwrap();

    let mut conn = migration_connection(&database.config).await;
    let captured: String = sqlx::query_scalar(
        "select split_part(config, '=', 2)
         from pg_catalog.pg_proc p
         cross join lateral unnest(p.proconfig) config
         where p.oid = 'etl.describe_table_columns(pg_catalog.oid)'::regprocedure
           and split_part(config, '=', 1) = 'extra_float_digits'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(captured, TEST_MIGRATION_OPTIONS.extra_float_digits.to_string());
}

/// Replacing a SECURITY DEFINER function retains its owner, even when another
/// admin installs new private helpers needed by that function.
#[tokio::test(flavor = "multi_thread")]
async fn source_upgrade_preserves_trigger_owner_helper_access() {
    let database = spawn_unmigrated_database().await;
    let mut conn = migration_connection(&database.config).await;
    source_migrator().run_to(SOURCE_PRE_SYNTHETIC_RELATION_VERSION, &mut conn).await.unwrap();
    drop(conn);

    let owner = format!("ddl_owner_{}", Uuid::new_v4().simple());
    let quoted_owner = quote_identifier(&owner);
    let client = database.client.as_ref().unwrap();
    client
        .batch_execute(&format!(
            "create role {quoted_owner};
             grant usage on schema etl to {quoted_owner};
             grant execute on function etl.describe_table_schema(oid), \
             etl.describe_table_identity(oid) to {quoted_owner};
             create table public.owner_table (id integer primary key);
             create publication owner_pub for table public.owner_table;
             alter function etl.emit_schema_change_messages() owner to {quoted_owner};
             alter table public.owner_table add column before_upgrade integer"
        ))
        .await
        .unwrap();

    run_source_migrations(&database.config).await.unwrap();

    let retained_owner: String = client
        .query_one(
            "select pg_catalog.pg_get_userbyid(proowner) from pg_catalog.pg_proc where oid = \
             'etl.emit_schema_change_messages()'::pg_catalog.regprocedure",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(retained_owner, owner);
    client
        .batch_execute(
            "alter table public.owner_table add column after_upgrade integer;
             alter publication owner_pub set (publish = 'insert')",
        )
        .await
        .unwrap();

    client
        .batch_execute(&format!(
            "alter function etl.emit_schema_change_messages() owner to current_user;
             drop owned by {quoted_owner}; drop role {quoted_owner}"
        ))
        .await
        .unwrap();
}

/// Admin-installed source upgrades retain role access without exposing helpers
/// to PUBLIC or granting readers access to trigger-only functions.
#[tokio::test(flavor = "multi_thread")]
async fn source_upgrade_preserves_reader_helper_grants() {
    let database = spawn_unmigrated_database().await;
    let mut conn = migration_connection(&database.config).await;
    // Start with exactly the source migrations shipped before synthetic DDL.
    source_migrator().run_to(SOURCE_PRE_SYNTHETIC_RELATION_VERSION, &mut conn).await.unwrap();
    drop(conn);
    assert!(
        query_bool(&database, "select to_regprocedure('etl.describe_table_columns(oid)') is null",)
            .await
    );

    let reader = format!("ddl reader's \"{}\"", Uuid::new_v4().simple());
    let member = format!("ddl_member_{}", Uuid::new_v4().simple());
    let quoted_reader = quote_identifier(&reader);
    let quoted_member = quote_identifier(&member);
    let client = database.client.as_ref().unwrap();
    client
        .batch_execute(&format!(
            "create role {quoted_reader};
             create role {quoted_member};
             grant {quoted_reader} to {quoted_member};
             grant usage on schema etl to {quoted_reader};
             grant execute on function etl.describe_table_schema(oid) to {quoted_reader} with \
             grant option;
             grant execute on function etl.describe_table_identity(oid) to {quoted_reader};
             create table public.reader_table (id integer primary key, value integer);
             create publication reader_pub for table public.reader_table"
        ))
        .await
        .unwrap();

    run_source_migrations(&database.config).await.unwrap();

    for signature in
        ["etl.describe_table_columns(oid)", "etl.describe_table_publication_columns(oid,text)"]
    {
        let privileges = client
            .query_one(
                "select pg_catalog.has_function_privilege($1, $3, 'execute with grant option'),
                        pg_catalog.has_function_privilege($2, $3, 'execute')",
                &[&reader, &member, &signature],
            )
            .await
            .unwrap();
        assert!(privileges.get::<_, bool>(0));
        assert!(privileges.get::<_, bool>(1));
    }
    let public_access = query_bool(
        &database,
        "select exists (
            select 1 from pg_catalog.pg_proc p
            cross join lateral pg_catalog.aclexplode(p.proacl) acl
            where p.oid in (
                'etl.describe_table_columns(oid)'::pg_catalog.regprocedure,
                'etl.describe_table_publication_columns(oid,text)'::pg_catalog.regprocedure
            ) and acl.grantee = 0
        )",
    )
    .await;
    assert!(!public_access);
    let private_helpers = client
        .query_one(
            "select pg_catalog.has_function_privilege($1, \
             'etl.publication_table_candidates(oid)', 'execute') or \
             pg_catalog.has_function_privilege($1, 'etl.emit_schema_change_messages()', 'execute')",
            &[&member],
        )
        .await
        .unwrap();
    assert!(!private_helpers.get::<_, bool>(0));

    client.batch_execute(&format!("set role {quoted_member}")).await.unwrap();
    let result = client
        .query_one(
            "select (select count(*) from \
             etl.describe_table_columns('public.reader_table'::regclass)),
                    etl.describe_table_identity('public.reader_table'::regclass) ? \
             'replica_identity_attnums',
                    (select column_attnums is null from etl.describe_table_publication_columns(
                        'public.reader_table'::regclass, 'reader_pub'))",
            &[],
        )
        .await;
    client.batch_execute("reset role").await.unwrap();
    let row = result.unwrap();
    assert_eq!(row.get::<_, i64>(0), 2);
    assert!(row.get::<_, bool>(1));
    assert!(row.get::<_, bool>(2));

    client
        .batch_execute(&format!(
            "drop owned by {quoted_reader}, {quoted_member};
             drop role {quoted_member}, {quoted_reader}"
        ))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn split_migrations_can_be_reverted_independently() {
    init_test_tracing();

    let database = spawn_unmigrated_database().await;

    let _store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    run_source_migrations(&database.config).await.unwrap();

    assert!(source_helper_exists(&database).await);
    assert!(postgres_store_table_exists(&database).await);
    assert_eq!(applied_migration_versions(&database).await, all_split_migration_versions());

    let client = database.client.as_ref().expect("database client should be initialized");
    let identity_reports_deferrability = "select etl.describe_table_identity(
            'etl.replication_state'::pg_catalog.regclass
        ) ? 'primary_key_condeferrable'";
    assert!(query_bool(&database, identity_reports_deferrability).await);

    let protocol_identity = "select etl.describe_table_identity(
        'etl.replication_state'::pg_catalog.regclass
    ) ? 'replica_identity_attnums'";
    let publication_helper = "select to_regprocedure(
        'etl.describe_table_publication_columns(oid,text)'
    ) is not null";
    let publication_candidates = "select to_regprocedure(
        'etl.publication_table_candidates(oid)'
    ) is not null";
    let decoder_columns = "select to_regprocedure(
        'etl.describe_table_columns(oid)'
    ) is not null";
    assert!(query_bool(&database, protocol_identity).await);
    assert!(query_bool(&database, publication_helper).await);
    assert!(query_bool(&database, publication_candidates).await);
    assert!(query_bool(&database, decoder_columns).await);

    let stored_missing_value = "select exists (
        select 1 from pg_catalog.pg_attribute
        where attrelid = 'etl.table_columns'::pg_catalog.regclass
          and attname = 'missing_value_expression'
          and not attnotnull and not attisdropped
    )";
    assert!(query_bool(&database, stored_missing_value).await);
    let mut conn = migration_connection(&database.config).await;
    postgres_store_migrator().undo(&mut conn, 20261002120100).await.unwrap();
    drop(conn);
    assert!(!query_bool(&database, stored_missing_value).await);
    assert!(query_bool(&database, decoder_columns).await);
    let _upgraded_store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    assert!(query_bool(&database, stored_missing_value).await);

    let mut conn = migration_connection(&database.config).await;
    source_migrator().undo(&mut conn, SOURCE_PRE_SYNTHETIC_RELATION_VERSION).await.unwrap();
    drop(conn);
    assert!(!query_bool(&database, protocol_identity).await);
    assert!(!query_bool(&database, publication_helper).await);
    assert!(!query_bool(&database, publication_candidates).await);
    assert!(!query_bool(&database, decoder_columns).await);

    let mut conn = migration_connection(&database.config).await;
    source_migrator().undo(&mut conn, SOURCE_PRE_PRIMARY_KEY_DEFERRABILITY_VERSION).await.unwrap();
    drop(conn);
    assert!(!query_bool(&database, identity_reports_deferrability).await);

    let mut conn = migration_connection(&database.config).await;
    source_migrator().undo(&mut conn, SOURCE_PRE_PUBLICATION_SCHEMA_CHANGE_VERSION).await.unwrap();
    drop(conn);

    let tags: Vec<String> = client
        .query_one(
            "select evttags
            from pg_catalog.pg_event_trigger
            where evtname = 'supabase_etl_ddl_message_trigger'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(tags, vec!["ALTER TABLE"]);

    let function_definition: String = client
        .query_one(
            "select pg_catalog.pg_get_functiondef(
                'etl.emit_schema_change_messages()'::pg_catalog.regprocedure
            )",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!function_definition.contains("publication_name"));

    let mut conn = migration_connection(&database.config).await;
    source_migrator().undo(&mut conn, 0).await.unwrap();
    drop(conn);

    assert!(!source_helper_exists(&database).await);
    assert!(postgres_store_table_exists(&database).await);
    assert_eq!(applied_migration_versions(&database).await, postgres_store_migration_versions());

    let mut conn = migration_connection(&database.config).await;
    postgres_store_migrator().undo(&mut conn, 0).await.unwrap();
    drop(conn);

    assert!(!source_helper_exists(&database).await);
    assert!(!postgres_store_table_exists(&database).await);
    assert_eq!(applied_migration_versions(&database).await, Vec::<i64>::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn source_migrations_wait_for_concurrent_etl_schema_creation() {
    init_test_tracing();

    let database = spawn_unmigrated_database().await;

    // Hold the schema creation open the way a concurrently starting pipeline
    // does: lock taken, catalog row written, transaction not yet committed.
    let options: PgConnectOptions = database.config.with_db(Some(&TEST_MIGRATION_OPTIONS));
    let mut holder = PgConnection::connect_with(&options).await.unwrap();
    let mut creating = holder.begin().await.unwrap();
    sqlx::query("select pg_advisory_xact_lock($1)")
        .bind(ETL_SCHEMA_LOCK_ID)
        .execute(&mut *creating)
        .await
        .unwrap();
    creating.execute("create schema if not exists etl;").await.unwrap();

    let config = database.config.clone();
    let mut migrations = tokio::spawn(async move { run_source_migrations(&config).await });

    // The migrations must queue behind the lock instead of failing on the
    // uncommitted schema.
    let outcome = tokio::time::timeout(Duration::from_millis(500), &mut migrations).await;
    assert!(
        outcome.is_err(),
        "source migrations completed while the schema creation was still open"
    );

    creating.commit().await.unwrap();
    migrations.await.unwrap().unwrap();
    drop(holder);

    assert!(source_helper_exists(&database).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_source_migrations_bootstrap_a_fresh_database() {
    init_test_tracing();

    let database = spawn_unmigrated_database().await;

    let starts = (0..8)
        .map(|_| {
            let config = database.config.clone();
            tokio::spawn(async move { run_source_migrations(&config).await })
        })
        .collect::<Vec<_>>();
    for start in starts {
        start.await.unwrap().unwrap();
    }

    assert!(source_helper_exists(&database).await);
    assert_eq!(applied_migration_versions(&database).await, source_migration_versions());
}
