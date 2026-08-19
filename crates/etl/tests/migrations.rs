use std::{collections::BTreeSet, sync::LazyLock};

use etl::{
    config::{
        BatchConfig, IntoConnectOptions, InvalidatedSlotBehavior, MemoryBackpressureConfig,
        PgConnectionConfig, PgConnectionOptions, PipelineConfig, TableSyncCopyConfig,
    },
    pipeline::Pipeline,
    postgres::migrations::run_source_migrations,
    store::{MemoryStore, PostgresStore},
    test_utils::memory_destination::MemoryDestination,
};
use etl_postgres::{test_utils::local_tls_config_from_env, tokio::test_utils::PgDatabase};
use etl_telemetry::tracing::init_test_tracing;
use sqlx::{Connection, Executor, PgConnection, migrate::Migrator, postgres::PgConnectOptions};
use tokio_postgres::Client;
use uuid::Uuid;

const DEFAULT_DATABASE_HOST: &str = "localhost";
const DEFAULT_DATABASE_PORT: &str = "5430";
const DEFAULT_DATABASE_USERNAME: &str = "postgres";
const DEFAULT_DATABASE_PASSWORD: &str = "postgres";
const POSTGRES_STORE_BASE_VERSION: i64 = 20250827000000;
const POSTGRES_STORE_PRE_COMPOSITE_SNAPSHOT_VERSION: i64 = 20260610120000;
const POSTGRES_STORE_PRE_CREATING_STATUS_VERSION: i64 = 20260810120000;
const APP_NAME_TEST_MIGRATIONS: &str = "supabase_etl_test_migrations";

static TEST_MIGRATION_OPTIONS: LazyLock<PgConnectionOptions> =
    LazyLock::new(|| PgConnectionOptions::builder(APP_NAME_TEST_MIGRATIONS).build());

fn local_pg_connection_config() -> PgConnectionConfig {
    PgConnectionConfig {
        host: std::env::var("TESTS_DATABASE_HOST").unwrap_or(DEFAULT_DATABASE_HOST.into()),
        hostaddr: None,
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
        replication_lag_refresh_interval_ms: 10000,
        memory_backpressure: Some(MemoryBackpressureConfig::default()),
        table_sync_copy: TableSyncCopyConfig::default(),
        invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
        run_source_migrations: true,
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
    let previous_source_version =
        source_migration_versions().into_iter().rev().nth(1).expect("a previous migration exists");
    let mut conn = migration_connection(&database.config).await;
    source_migrator().undo(&mut conn, previous_source_version).await.unwrap();
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
