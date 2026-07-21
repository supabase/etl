use std::sync::Arc;

use etl::{
    event::EventType,
    pipeline::PipelineId,
    schema::TableName,
    store::TableStateType,
    test_utils::{
        database::{spawn_source_database, test_table_name},
        event::EventCondition,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
    },
};
use etl_destinations::snowflake::{
    AuthManager, Client, Destination, HttpExchanger, OffsetToken, SqlClient,
    test_utils::{load_test_config, query_rows},
};
use etl_postgres::tokio::test_utils::TableModification;
use rand::random;
use serde_json::Value;
use tokio::time::{Duration, sleep};

use super::common::{build_auth, poll_destination_offset, with_table_cleanup};

const QUERY_POLL_INTERVAL: Duration = Duration::from_secs(1);
const QUERY_MAX_ATTEMPTS: usize = 90;
const DESTINATION_OFFSET_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DESTINATION_OFFSET_MAX_ATTEMPTS: usize = 90;

fn snowflake_table_name(src_table: &TableName) -> String {
    let escaped_schema = src_table.schema.replace('_', "__");
    let escaped_table = src_table.name.replace('_', "__");
    format!("{escaped_schema}_{escaped_table}").to_uppercase()
}

async fn query_default_rows(
    sql: &SqlClient<AuthManager<HttpExchanger>>,
    database: &str,
    schema: &str,
    table: &str,
) -> Vec<Vec<Value>> {
    let fqn = format!("\"{database}\".\"{schema}\".\"{table}\"");
    query_rows(
        sql,
        &format!(
            "select \"id\"::varchar, \"status\", \"score\"::varchar, \"active\"::varchar from \
             {fqn} order by \"id\""
        ),
    )
    .await
    .expect("query for defaulted rows failed")
}

/// Queries the initial row copied before the source schema change.
async fn query_initial_row(
    sql: &SqlClient<AuthManager<HttpExchanger>>,
    database: &str,
    schema: &str,
    table: &str,
) -> Vec<Vec<Value>> {
    let fqn = format!("\"{database}\".\"{schema}\".\"{table}\"");
    query_rows(sql, &format!("select \"id\"::varchar, \"name\" from {fqn} where \"id\" = '1'"))
        .await
        .expect("query for initial row failed")
}

/// Waits until the pre-existing source row is visible in Snowflake.
async fn wait_for_initial_row(
    sql: &SqlClient<AuthManager<HttpExchanger>>,
    database: &str,
    schema: &str,
    table: &str,
) -> Vec<Vec<Value>> {
    let expected = vec![vec![serde_json::json!("1"), serde_json::json!("Alice")]];
    let mut last_rows = Vec::new();

    for attempt in 0..QUERY_MAX_ATTEMPTS {
        if attempt > 0 {
            sleep(QUERY_POLL_INTERVAL).await;
        }

        last_rows = query_initial_row(sql, database, schema, table).await;
        if last_rows == expected {
            return last_rows;
        }
    }

    last_rows
}

async fn wait_for_default_rows(
    sql: &SqlClient<AuthManager<HttpExchanger>>,
    database: &str,
    schema: &str,
    table: &str,
    expected: &[Vec<Value>],
) -> Vec<Vec<Value>> {
    let mut last_rows = Vec::new();

    for attempt in 0..QUERY_MAX_ATTEMPTS {
        if attempt > 0 {
            sleep(QUERY_POLL_INTERVAL).await;
        }

        last_rows = query_default_rows(sql, database, schema, table).await;
        if last_rows == expected {
            return last_rows;
        }
    }

    last_rows
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Snowflake credentials"]
async fn schema_change_add_column_defaults() {
    let database = spawn_source_database().await;
    let table_name =
        test_table_name(&format!("snowflake_defaults_{}", uuid::Uuid::new_v4().simple()));
    let table_id = database
        .create_table(table_name.clone(), true, &[("name", "text not null")])
        .await
        .expect("failed to create source table");

    let publication_name = "test_pub_snowflake_defaults";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("failed to create publication");
    database
        .run_sql(&format!(
            "insert into {} (name) values ('Alice')",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert initial source row");

    let config = load_test_config().clone_without_credentials();
    let auth = build_auth();
    let sql = SqlClient::new(
        config.clone_without_credentials(),
        Arc::clone(&auth),
        reqwest::Client::new(),
    );
    let snowflake_table = snowflake_table_name(&table_name);

    with_table_cleanup(&sql, &[&snowflake_table], || async {
        let store = NotifyingStore::new();
        let pipeline_id: PipelineId = random();
        let client = Client::new(Arc::clone(&auth), pipeline_id);
        let raw_destination = Destination::new(client, store.clone());
        let destination_for_polling = raw_destination.clone();
        let destination = TestDestinationWrapper::wrap(raw_destination);
        let mut pipeline = create_pipeline(
            &database.config,
            pipeline_id,
            publication_name.to_owned(),
            store.clone(),
            destination.clone(),
        );
        let table_ready_notify =
            store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

        pipeline.start().await.unwrap();
        table_ready_notify.notified().await;

        let copy_offset = OffsetToken::new(0_u64.into(), 1);
        let committed = poll_destination_offset(
            &destination_for_polling,
            table_id,
            &copy_offset,
            DESTINATION_OFFSET_POLL_INTERVAL,
            DESTINATION_OFFSET_MAX_ATTEMPTS,
        )
        .await;
        assert_eq!(committed, Some(copy_offset), "initial data should commit before source DDL");

        let initial_rows =
            wait_for_initial_row(&sql, config.database(), config.schema(), &snowflake_table).await;
        assert_eq!(initial_rows, vec![vec![serde_json::json!("1"), serde_json::json!("Alice")]]);

        let events_notify = destination
            .wait_for_events(vec![
                EventCondition::TableCount(EventType::Relation, table_id, 1),
                EventCondition::TableCount(EventType::Insert, table_id, 1),
            ])
            .await;

        database
            .alter_table(
                table_name.clone(),
                &[
                    TableModification::AddColumn {
                        name: "status",
                        data_type: "text default 'new'::text",
                    },
                    TableModification::AddColumn { name: "score", data_type: "integer default 15" },
                    TableModification::AddColumn {
                        name: "active",
                        data_type: "boolean default true",
                    },
                ],
            )
            .await
            .expect("failed to alter source table");
        database
            .run_sql(&format!(
                "insert into {} (name) values ('Bob')",
                table_name.as_quoted_identifier()
            ))
            .await
            .expect("failed to insert defaulted source row");

        events_notify.notified().await;
        pipeline.shutdown_and_wait().await.unwrap();

        let expected = vec![
            vec![
                serde_json::json!("1"),
                serde_json::json!("new"),
                serde_json::json!("15"),
                serde_json::json!("true"),
            ],
            vec![
                serde_json::json!("2"),
                serde_json::json!("new"),
                serde_json::json!("15"),
                serde_json::json!("true"),
            ],
        ];
        let rows = wait_for_default_rows(
            &sql,
            config.database(),
            config.schema(),
            &snowflake_table,
            &expected,
        )
        .await;
        assert_eq!(rows, expected);
    })
    .await;
}
