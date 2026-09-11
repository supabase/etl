use std::time::Duration;

use etl_api::startup::{Application, get_connection_pool};
use etl_config::shared::PgConnectionConfig;
use etl_postgres::sqlx::test_utils::{create_pg_database, drop_pg_database};
use pg_escape::{quote_identifier, quote_literal};
use sqlx::{AssertSqlSafe, Connection, PgPool};
use tokio::time::timeout;

use crate::support::database::get_test_db_config;

/// Changes a session default only for this test's isolated database.
async fn set_database_option(
    pool: &PgPool,
    config: &PgConnectionConfig,
    option: &str,
    value: &str,
) {
    let query = format!(
        "alter database {} set {} = {}",
        quote_identifier(&config.name),
        quote_identifier(option),
        quote_literal(value),
    );
    sqlx::query(AssertSqlSafe(query)).execute(pool).await.unwrap();
}

/// Every physical runtime connection receives the SQL limits and preserves
/// other defaults.
#[tokio::test]
async fn metadata_pool_applies_timeouts_to_each_connection() {
    let config = get_test_db_config();
    let admin_pool = create_pg_database(&config).await;
    set_database_option(&admin_pool, &config, "statement_timeout", "2min").await;
    set_database_option(&admin_pool, &config, "lock_timeout", "1min").await;
    set_database_option(&admin_pool, &config, "idle_in_transaction_session_timeout", "90s").await;

    let pool = get_connection_pool(&config);
    let mut first = pool.acquire().await.unwrap();
    let mut second = pool.acquire().await.unwrap();
    for connection in [&mut first, &mut second] {
        let settings: (i32, i32, i32) = sqlx::query_as(
            "select
                (select setting::int from pg_settings where name = 'statement_timeout'),
                (select setting::int from pg_settings where name = 'lock_timeout'),
                (select setting::int from pg_settings
                 where name = 'idle_in_transaction_session_timeout')",
        )
        .fetch_one(&mut **connection)
        .await
        .unwrap();
        assert_eq!(settings, (30_000, 10_000, 90_000));
    }

    drop(first);
    drop(second);
    pool.close().await;
    admin_pool.close().await;
    drop_pg_database(&config).await;
}

/// A slow statement fails at the server and its connection works after
/// rollback.
#[tokio::test]
async fn metadata_statement_timeout_allows_connection_reuse_after_rollback() {
    let config = get_test_db_config();
    let admin_pool = create_pg_database(&config).await;
    let pool = get_connection_pool(&config);
    let mut connection = pool.acquire().await.unwrap();
    let mut transaction = connection.begin().await.unwrap();

    let error = timeout(
        Duration::from_secs(45),
        sqlx::query("select pg_sleep(60)").execute(&mut *transaction),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("57014"));
    transaction.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i32>("select 1").fetch_one(&mut *connection).await.unwrap(),
        1
    );

    drop(connection);
    pool.close().await;
    admin_pool.close().await;
    drop_pg_database(&config).await;
}

/// Lock contention fails independently of the longer statement timeout.
#[tokio::test]
async fn metadata_lock_timeout_allows_connection_reuse_after_rollback() {
    let config = get_test_db_config();
    let admin_pool = create_pg_database(&config).await;
    let mut holder = admin_pool.begin().await.unwrap();
    sqlx::query("select pg_advisory_xact_lock(710)").execute(&mut *holder).await.unwrap();

    let pool = get_connection_pool(&config);
    let mut connection = pool.acquire().await.unwrap();
    let mut transaction = connection.begin().await.unwrap();
    let error = timeout(
        Duration::from_secs(20),
        sqlx::query("select pg_advisory_xact_lock(710)").execute(&mut *transaction),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("55P03"));
    transaction.rollback().await.unwrap();
    holder.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i32>("select 1").fetch_one(&mut *connection).await.unwrap(),
        1
    );

    drop(connection);
    pool.close().await;
    admin_pool.close().await;
    drop_pg_database(&config).await;
}

/// Migration connections retain inherited limits instead of using runtime
/// defaults.
#[tokio::test]
async fn migrations_preserve_inherited_timeouts() {
    let config = get_test_db_config();
    let admin_pool = create_pg_database(&config).await;
    Application::migrate_database(config.clone()).await.unwrap();
    set_database_option(&admin_pool, &config, "statement_timeout", "0").await;
    set_database_option(&admin_pool, &config, "lock_timeout", "0").await;

    for (option, code) in [("statement_timeout", "57014"), ("lock_timeout", "55P03")] {
        set_database_option(&admin_pool, &config, option, "100ms").await;
        let mut holder = admin_pool.begin().await.unwrap();
        sqlx::query("lock table _sqlx_migrations in access exclusive mode")
            .execute(&mut *holder)
            .await
            .unwrap();

        let error = timeout(Duration::from_secs(5), Application::migrate_database(config.clone()))
            .await
            .unwrap()
            .unwrap_err();
        let database_error =
            error.chain().find_map(|error| error.downcast_ref::<sqlx::Error>()).unwrap();
        assert_eq!(database_error.as_database_error().unwrap().code().as_deref(), Some(code));

        holder.rollback().await.unwrap();
        set_database_option(&admin_pool, &config, option, "0").await;
        Application::migrate_database(config.clone()).await.unwrap();
    }

    admin_pool.close().await;
    drop_pg_database(&config).await;
}
