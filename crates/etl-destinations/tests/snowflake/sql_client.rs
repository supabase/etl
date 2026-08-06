use etl_destinations::snowflake::{
    AuthManager, HttpExchanger, SqlClient,
    test_utils::{load_test_config, query_rows},
};
use uuid::Uuid;

use super::common::{build_auth, with_table_cleanup};

fn build_sql_client() -> SqlClient<AuthManager<HttpExchanger>> {
    let config = load_test_config();
    let auth = build_auth();
    SqlClient::new(config, auth, reqwest::Client::new())
}

#[tokio::test]
#[ignore = "requires Snowflake credentials — see etl-destinations/src/snowflake/README.md"]
async fn ddl_lifecycle() {
    let client = build_sql_client();
    let table = format!("etl_test_{}", uuid::Uuid::new_v4().simple());

    with_table_cleanup(&client, &[&table], || async {
        client
            .create_table_if_not_exists(&table, r#""id" NUMBER(10,0), "name" VARCHAR"#)
            .await
            .expect("create table failed");

        assert!(
            client.table_exists(&table).await.expect("table_exists failed"),
            "table should exist after creation"
        );

        client.truncate_table(&table, Uuid::new_v4()).await.expect("truncate failed");

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
async fn truncate_request_id_is_idempotent_across_clients() {
    let client = build_sql_client();
    let config = load_test_config();
    let table = format!("etl_test_{}", Uuid::new_v4().simple());
    let fqn = format!("\"{}\".\"{}\".\"{table}\"", config.database(), config.schema());

    with_table_cleanup(&client, &[&table], || async {
        client
            .create_table_if_not_exists(&table, r#""id" NUMBER(10,0)"#)
            .await
            .expect("create table failed");
        client
            .execute_ddl(&format!("insert into {fqn} values (1)"))
            .await
            .expect("initial insert failed");

        let request_id = Uuid::new_v4();
        client.truncate_table(&table, request_id).await.expect("initial truncate failed");
        client
            .execute_ddl(&format!("insert into {fqn} values (2)"))
            .await
            .expect("post-truncate insert failed");

        let restarted_client = build_sql_client();
        restarted_client
            .truncate_table(&table, request_id)
            .await
            .expect("same-ID truncate retry failed");
        let rows = query_rows(&restarted_client, &format!("select \"id\" from {fqn}"))
            .await
            .expect("query after same-ID retry failed");
        assert_eq!(rows, vec![vec![serde_json::json!("2")]]);

        restarted_client
            .truncate_table(&table, Uuid::new_v4())
            .await
            .expect("fresh-ID truncate failed");
        let rows = query_rows(&restarted_client, &format!("select \"id\" from {fqn}"))
            .await
            .expect("query after fresh-ID truncate failed");
        assert!(rows.is_empty());
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

    with_table_cleanup(&client, &[&table], || async {
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

    with_table_cleanup(&client, &[&table], || async {
        client
            .create_table_if_not_exists(&table, r#""id" NUMBER(10,0), "name" VARCHAR"#)
            .await
            .expect("create table failed");

        client.add_column(&table, "email", "VARCHAR", None).await.expect("add column failed");

        client.rename_column(&table, "email", "user_email").await.expect("rename column failed");

        client.drop_column(&table, "user_email").await.expect("drop column failed");
    })
    .await;
}
