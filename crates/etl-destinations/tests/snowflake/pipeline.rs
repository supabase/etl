use std::sync::Arc;

use etl::{
    event::{Event, EventType},
    failpoints::{APPLY_LOOP_EXPIRE_DURABILITY_FP, etl_fail_point_active_for_parameter},
    pipeline::PipelineId,
    schema::TableName,
    test_utils::{
        database::{spawn_source_database, test_table_name},
        event::EventCondition,
        notifying_store::NotifyingStore,
        pipeline::{PipelineBuilder, create_pipeline},
        test_destination_wrapper::TestDestinationWrapper,
    },
};
use etl_destinations::snowflake::{
    AuthManager, Client, Destination, HttpExchanger, OffsetToken, SqlClient,
    test_utils::{load_test_config, query_rows},
};
use etl_postgres::tokio::test_utils::TableModification;
use fail::FailScenario;
use rand::random;
use tokio::time::Duration;

use crate::snowflake::common::{
    build_auth, poll_destination_offset, quote, wait_for_replication_checkpoint, with_table_cleanup,
};

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
) -> Vec<Vec<serde_json::Value>> {
    let fqn = format!("\"{database}\".\"{schema}\".\"{table}\"");
    query_rows(
        sql,
        &format!(
            "select \"id\"::varchar, \"replaced\", \"status\", \"score\"::varchar, \
             \"active\"::varchar from {fqn} order by \"id\""
        ),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Snowflake credentials"]
async fn schema_change_batched_operations() {
    let database = spawn_source_database().await;
    let table_name =
        test_table_name(&format!("snowflake_defaults_{}", uuid::Uuid::new_v4().simple()));
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text not null"), ("replaced", "integer not null")],
        )
        .await
        .unwrap();

    let publication_name = "test_pub_snowflake_defaults";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();
    database
        .run_sql(&format!(
            "insert into {} (name, replaced) values ('Alice', 25)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

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
        let table_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;

        pipeline.start().await.unwrap();
        table_sync_complete_notify.notified().await;

        let copy_offset = OffsetToken::new(0_u64.into(), 1);
        let committed = poll_destination_offset(
            &destination_for_polling,
            table_id,
            &copy_offset,
            DESTINATION_OFFSET_POLL_INTERVAL,
            DESTINATION_OFFSET_MAX_ATTEMPTS,
        )
        .await;
        // The initial copy must be durable before schema evolution starts.
        assert_eq!(committed, Some(copy_offset));

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
                    TableModification::DropColumn { name: "replaced" },
                    TableModification::AddColumn { name: "replaced", data_type: "text" },
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
            .unwrap();
        database
            .run_sql(&format!(
                "insert into {} (name, replaced) values ('Bob', 'new-replaced')",
                table_name.as_quoted_identifier()
            ))
            .await
            .unwrap();

        events_notify.notified().await;
        let expected_offset = destination
            .get_events()
            .await
            .into_iter()
            .rev()
            .find_map(|event| match event {
                Event::Insert(event) if event.replicated_table_schema.id() == table_id => {
                    Some(OffsetToken::new(event.commit_lsn, event.tx_ordinal))
                }
                _ => None,
            })
            .unwrap();

        pipeline.shutdown_and_wait().await.unwrap();

        let committed = poll_destination_offset(
            &destination_for_polling,
            table_id,
            &expected_offset,
            DESTINATION_OFFSET_POLL_INTERVAL,
            DESTINATION_OFFSET_MAX_ATTEMPTS,
        )
        .await;
        // The post-DDL row must be durable before querying destination state.
        assert_eq!(committed, Some(expected_offset));

        let expected = vec![
            vec![
                serde_json::json!("1"),
                serde_json::Value::Null,
                serde_json::json!("new"),
                serde_json::json!("15"),
                serde_json::json!("true"),
            ],
            vec![
                serde_json::json!("2"),
                serde_json::json!("new-replaced"),
                serde_json::json!("new"),
                serde_json::json!("15"),
                serde_json::json!("true"),
            ],
        ];
        let rows =
            query_default_rows(&sql, config.database(), config.schema(), &snowflake_table).await;
        assert_eq!(rows, expected);
    })
    .await;
}

/// Confirms a deadline-driven cumulative barrier with live Snowflake and a
/// tiny source table. Idle settlement is disabled for the duration of the test.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local PostgreSQL and Snowflake test credentials"]
async fn durability_deadline_advances_live_snowflake_checkpoint() {
    let _scenario = FailScenario::setup();
    let database = spawn_source_database().await;
    let table_name = test_table_name(&format!("deadline_{}", uuid::Uuid::new_v4().simple()));
    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();
    let publication = "deadline_publication";
    database.create_publication(publication, std::slice::from_ref(&table_name)).await.unwrap();
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
        let raw_destination = Destination::new(Client::new(auth, pipeline_id), store.clone());
        let destination_for_polling = raw_destination.clone();
        let destination = TestDestinationWrapper::wrap(raw_destination);
        let mut pipeline = PipelineBuilder::new(
            database.config.clone(),
            pipeline_id,
            publication.to_owned(),
            store.clone(),
            destination.clone(),
        )
        .with_settle_durable_interval_ms(Some(600_000))
        .build();
        let synced = store.notify_on_table_sync_complete(table_id).await;

        pipeline.start().await.unwrap();

        synced.notified().await;
        database.run_sql("analyze").await.unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(120), async {
            // The first insert warms the relation/schema path. The second is
            // pure DML and leaves the small Snowflake window accepted.
            for value in [1_i32, 2] {
                let written = destination
                    .wait_for_events(vec![EventCondition::TableCount(
                        EventType::Insert,
                        table_id,
                        u64::try_from(value).unwrap(),
                    )])
                    .await;
                fail::cfg(APPLY_LOOP_EXPIRE_DURABILITY_FP, "1*return(apply)").unwrap();

                database.insert_values(table_name.clone(), &["value"], &[&value]).await.unwrap();

                written.notified().await;
                let events = destination.get_events().await;
                let commit_end_lsn = events
                    .iter()
                    .rev()
                    .find_map(|event| match event {
                        Event::Commit(commit) => Some(commit.end_lsn),
                        _ => None,
                    })
                    .unwrap();
                wait_for_replication_checkpoint(&store, commit_end_lsn).await;
                if value == 2 {
                    // The single-use hook must have been consumed by an
                    // Accepted result; a direct Durable result would leave it
                    // armed and would not prove the deadline path.
                    assert!(!etl_fail_point_active_for_parameter(
                        APPLY_LOOP_EXPIRE_DURABILITY_FP,
                        "apply"
                    ));
                }
            }
            let events = destination.get_events().await;
            events
                .iter()
                .rev()
                .find_map(|event| match event {
                    Event::Insert(insert) => {
                        Some(OffsetToken::new(insert.commit_lsn, insert.tx_ordinal))
                    }
                    _ => None,
                })
                .unwrap()
        })
        .await;
        pipeline.shutdown_and_wait().await.unwrap();
        let expected_offset = outcome.unwrap();
        assert_eq!(
            destination_for_polling.fetch_committed_offset(table_id).await.unwrap(),
            Some(expected_offset)
        );
        let rows = query_rows(
            &sql,
            &format!(
                "select count(*) from {}.{}.{}",
                quote(config.database()),
                quote(config.schema()),
                quote(&snowflake_table)
            ),
        )
        .await
        .unwrap();
        assert_eq!(rows, vec![vec![serde_json::json!("2")]]);
    })
    .await;
    assert!(!sql.table_exists(&snowflake_table).await.unwrap());
}
