use std::sync::Arc;

use etl_destinations::snowflake::{
    AuthManager, HttpExchanger, SqlClient,
    test_utils::{load_test_config, load_test_private_key_path},
};
use futures::FutureExt;

fn build_sql_client() -> SqlClient<AuthManager<HttpExchanger>> {
    let config = load_test_config();
    let key_path = load_test_private_key_path();
    let auth = Arc::new(
        AuthManager::new(&config, key_path.to_str().unwrap(), None)
            .expect("AuthManager creation failed"),
    );
    SqlClient::new(config, auth, reqwest::Client::new())
}

/// Run `test_fn`, then always clean up `tables` regardless of success or
/// failure.
async fn with_cleanup<F, Fut>(
    client: &SqlClient<AuthManager<HttpExchanger>>,
    tables: &[&str],
    test_fn: F,
) where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let result = std::panic::AssertUnwindSafe(test_fn()).catch_unwind().await;

    for table in tables {
        let _ = client.drop_table(table).await;
    }

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

#[tokio::test]
#[ignore = "requires Snowflake credentials — see etl-destinations/src/snowflake/README.md"]
async fn ddl_lifecycle() {
    let client = build_sql_client();
    let table = format!("etl_test_{}", uuid::Uuid::new_v4().simple());

    with_cleanup(&client, &[&table], || async {
        client
            .create_table_if_not_exists(&table, r#""id" NUMBER(10,0), "name" VARCHAR"#)
            .await
            .expect("create table failed");

        assert!(
            client.table_exists(&table).await.expect("table_exists failed"),
            "table should exist after creation"
        );

        client.truncate_table(&table).await.expect("truncate failed");

        assert!(
            client.table_exists(&table).await.expect("table_exists failed"),
            "table should still exist after truncate"
        );

        client.drop_table(&table).await.expect("drop table failed");

        assert!(
            !client.table_exists(&table).await.expect("table_exists failed"),
            "table should not exist after drop"
        );
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials — see etl-destinations/src/snowflake/README.md"]
async fn table_exists_returns_false_for_nonexistent() {
    let client = build_sql_client();
    let table = format!("etl_nonexistent_{}", uuid::Uuid::new_v4().simple());

    assert!(
        !client.table_exists(&table).await.expect("table_exists failed"),
        "table that was never created should not exist"
    );
}

#[tokio::test]
#[ignore = "requires Snowflake credentials — see etl-destinations/src/snowflake/README.md"]
async fn create_table_idempotent() {
    let client = build_sql_client();
    let table = format!("etl_test_{}", uuid::Uuid::new_v4().simple());

    with_cleanup(&client, &[&table], || async {
        client
            .create_table_if_not_exists(&table, r#""id" NUMBER(10,0)"#)
            .await
            .expect("first create failed");

        client
            .create_table_if_not_exists(&table, r#""id" NUMBER(10,0)"#)
            .await
            .expect("second create should succeed silently");
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials — see etl-destinations/src/snowflake/README.md"]
async fn schema_evolution_ddl() {
    let client = build_sql_client();
    let table = format!("etl_test_{}", uuid::Uuid::new_v4().simple());

    with_cleanup(&client, &[&table], || async {
        client
            .create_table_if_not_exists(&table, r#""id" NUMBER(10,0), "name" VARCHAR"#)
            .await
            .expect("create table failed");

        client.add_column(&table, "email", "VARCHAR").await.expect("add column failed");

        client.rename_column(&table, "email", "user_email").await.expect("rename column failed");

        client.drop_column(&table, "user_email").await.expect("drop column failed");
    })
    .await;
}
