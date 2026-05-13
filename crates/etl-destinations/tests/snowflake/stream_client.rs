use std::sync::Arc;

use etl::types::{Cell, ColumnSchema, TableRow, Type};
use etl_destinations::snowflake::{
    AuthManager, CdcMeta, CdcOperation, Config, Error, HttpExchanger, OffsetToken,
    RestStreamClient, RowBatch, RowBatchBuilder, SqlClient, StreamClient,
    test_utils::{load_test_config, load_test_private_key_path},
};
use futures::FutureExt;

/// Poll channel_status until the channel's offset_token is Some, up to
/// `max_attempts` polls with `interval` between each. Returns the committed
/// offset token or None if it never appeared.
///
/// Normally, Snowflake commits data it received in some 20-30s, up to 90s (in
/// observed tests).
async fn poll_committed_offset(
    stream: &RestStreamClient<AuthManager<HttpExchanger>>,
    config: &Config,
    table: &str,
    channel: &str,
    interval: std::time::Duration,
    max_attempts: usize,
) -> Option<OffsetToken> {
    for _ in 0..max_attempts {
        tokio::time::sleep(interval).await;
        let status = stream
            .channel_status(&config.database, &config.schema, table, channel)
            .await
            .expect("channel_status failed");
        if status.offset_token.is_some() {
            return status.offset_token;
        }
    }
    None
}

fn build_clients(
    config: &Config,
) -> (RestStreamClient<AuthManager<HttpExchanger>>, SqlClient<AuthManager<HttpExchanger>>) {
    let key_path = load_test_private_key_path();
    let auth = Arc::new(
        AuthManager::new(config, key_path.to_str().unwrap(), None)
            .expect("AuthManager creation failed"),
    );
    let stream = RestStreamClient::new(
        config.account_url.clone(),
        Arc::clone(&auth),
        reqwest::Client::new(),
    );
    let sql = SqlClient::new(config.clone(), Arc::clone(&auth), reqwest::Client::new());
    (stream, sql)
}

fn build_batch(cols: &[ColumnSchema], rows: &[TableRow], offset: &OffsetToken) -> RowBatch {
    let mut builder = RowBatchBuilder::new();
    for row in rows {
        builder.push_row(cols, row, CdcMeta::new(CdcOperation::Insert, "0"), offset).unwrap();
    }
    builder.finish().unwrap().into_iter().next().unwrap()
}

/// Run `test_fn`, with clean up `tables` regardless of success or failure.
async fn with_table_cleanup<F, Fut>(
    sql: &SqlClient<AuthManager<HttpExchanger>>,
    tables: &[&str],
    test_fn: F,
) where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let result = std::panic::AssertUnwindSafe(test_fn()).catch_unwind().await;

    for table in tables {
        let _ = sql.drop_table(table).await;
    }

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn channel_open_insert_status_drop() {
    let config = load_test_config();
    let (stream, sql) = build_clients(&config);

    let table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let channel = format!("etl_test_{}_ch0", uuid::Uuid::new_v4().simple());

    with_table_cleanup(&sql, &[&table], || async {
        // Create test table.
        sql.create_table_if_not_exists(&table, r#""id" NUMBER(10,0), "name" VARCHAR"#)
            .await
            .expect("create table failed");

        // Open channel, no offset on fresh channel.
        let resp = stream
            .open_channel(&config.database, &config.schema, &table, &channel)
            .await
            .expect("open_channel failed");
        assert!(!resp.continuation_token.is_empty(), "expected non-empty continuation_token");
        assert!(resp.offset_token.is_none(), "unexpected offset on fresh channel");

        // Insert rows.
        let cols = [
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, None, true),
            ColumnSchema::new("name".into(), Type::TEXT, -1, 2, None, true),
        ];
        let offset: OffsetToken = "0000000000000001/0000000000000001".parse().unwrap();
        let batch = build_batch(
            &cols,
            &[
                TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())]),
                TableRow::new(vec![Cell::I32(2), Cell::String("Bob".into())]),
            ],
            &offset,
        );
        let resp = stream
            .insert_rows(
                &config.database,
                &config.schema,
                &table,
                &channel,
                &batch,
                &resp.continuation_token,
            )
            .await
            .expect("insert_rows failed");
        assert!(
            !resp.continuation_token.is_empty(),
            "expected non-empty continuation_token after insert"
        );

        // Poll status until offset is committed.
        let committed = poll_committed_offset(
            &stream,
            &config,
            &table,
            &channel,
            std::time::Duration::from_secs(5),
            18,
        )
        .await;
        assert_eq!(committed, Some(offset), "committed offset must match inserted offset");

        // Drop channel.
        stream
            .drop_channel(&config.database, &config.schema, &table, &channel)
            .await
            .expect("drop_channel failed");
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn channel_reopen_preserves_offset() {
    let config = load_test_config();
    let (stream, sql) = build_clients(&config);

    let table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let channel = format!("etl_test_{}_ch0", uuid::Uuid::new_v4().simple());

    with_table_cleanup(&sql, &[&table], || async {
        sql.create_table_if_not_exists(&table, r#""id" NUMBER(10,0), "name" VARCHAR"#)
            .await
            .expect("create table failed");

        // Open and insert.
        let open_resp = stream
            .open_channel(&config.database, &config.schema, &table, &channel)
            .await
            .expect("open_channel failed");

        let cols = [
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, None, true),
            ColumnSchema::new("name".into(), Type::TEXT, -1, 2, None, true),
        ];
        let offset: OffsetToken = "0000000000000001/0000000000000000".parse().unwrap();
        let batch = build_batch(
            &cols,
            &[TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())])],
            &offset,
        );
        stream
            .insert_rows(
                &config.database,
                &config.schema,
                &table,
                &channel,
                &batch,
                &open_resp.continuation_token,
            )
            .await
            .expect("insert_rows failed");

        // Poll until offset is committed.
        let committed = poll_committed_offset(
            &stream,
            &config,
            &table,
            &channel,
            std::time::Duration::from_secs(5),
            18,
        )
        .await;
        assert_eq!(committed, Some(offset.clone()), "committed offset must match inserted offset");

        // Reopen channel, idempotent, should return the committed offset.
        let reopen_resp = stream
            .open_channel(&config.database, &config.schema, &table, &channel)
            .await
            .expect("reopen channel failed");
        assert_eq!(
            reopen_resp.offset_token,
            Some(offset),
            "reopened channel must return the committed offset"
        );

        let _ = stream.drop_channel(&config.database, &config.schema, &table, &channel).await;
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn continuation_token() {
    let config = load_test_config();
    let (stream, sql) = build_clients(&config);

    let table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let channel = format!("etl_test_{}_ch0", uuid::Uuid::new_v4().simple());

    with_table_cleanup(&sql, &[&table], || async {
        sql.create_table_if_not_exists(&table, r#""id" NUMBER(10,0), "name" VARCHAR"#)
            .await
            .expect("create table failed");

        let resp = stream
            .open_channel(&config.database, &config.schema, &table, &channel)
            .await
            .expect("open_channel failed");

        // Batch 1
        let cols = [
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, None, true),
            ColumnSchema::new("name".into(), Type::TEXT, -1, 2, None, true),
        ];
        let offset1: OffsetToken = "0000000000000001/0000000000000000".parse().unwrap();
        let batch1 = build_batch(
            &cols,
            &[TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())])],
            &offset1,
        );
        let insert1 = stream
            .insert_rows(
                &config.database,
                &config.schema,
                &table,
                &channel,
                &batch1,
                &resp.continuation_token,
            )
            .await
            .expect("insert batch 1 failed");

        // Batch 2, uses continuation_token from batch 1.
        let offset2: OffsetToken = "0000000000000001/0000000000000001".parse().unwrap();
        let batch2 = build_batch(
            &cols,
            &[TableRow::new(vec![Cell::I32(2), Cell::String("Bob".into())])],
            &offset2,
        );
        let insert2 = stream
            .insert_rows(
                &config.database,
                &config.schema,
                &table,
                &channel,
                &batch2,
                &insert1.continuation_token,
            )
            .await
            .expect("insert batch 2 failed");

        assert_ne!(
            insert1.continuation_token, insert2.continuation_token,
            "continuation_token must advance after each batch"
        );

        // Batch 3: use STALE token from batch 1 (already consumed by batch 2).
        let offset3: OffsetToken = "0000000000000001/0000000000000002".parse().unwrap();
        let batch3 = build_batch(
            &cols,
            &[TableRow::new(vec![Cell::I32(3), Cell::String("Charlie".into())])],
            &offset3,
        );
        let result = stream
            .insert_rows(
                &config.database,
                &config.schema,
                &table,
                &channel,
                &batch3,
                &insert1.continuation_token,
            )
            .await;
        let err = result.expect_err("stale continuation_token should be rejected");
        assert!(
            matches!(err, Error::Snowpipe { status_code: 4, .. }),
            "expected Snowpipe NotFound (code 4), got: {err}"
        );

        // Cleanup
        let _ = stream.drop_channel(&config.database, &config.schema, &table, &channel).await;
    })
    .await;
}
