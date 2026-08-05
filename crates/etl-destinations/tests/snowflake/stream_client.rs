use std::sync::Arc;

use etl::{
    data::{Cell, TableRow},
    schema::{ColumnSchema, Type},
};
use etl_destinations::snowflake::{
    AuthManager, CdcMeta, CdcOperation, Config, Error, HttpExchanger, OffsetToken,
    RestStreamClient, RowBatch, RowBatchBuilder, SnowpipeError, SqlClient, StreamClient,
    test_utils::{load_test_config, query_rows},
};
use tokio::time::Duration;

use super::common::{build_auth, poll_stream_offset, with_table_cleanup};

fn build_clients(
    config: &Config,
) -> (RestStreamClient<AuthManager<HttpExchanger>>, SqlClient<AuthManager<HttpExchanger>>) {
    let auth = build_auth();
    let stream = RestStreamClient::new(
        config.account_url().to_owned(),
        Arc::clone(&auth),
        reqwest::Client::new(),
    );
    let sql = SqlClient::new(
        config.clone_without_credentials(),
        Arc::clone(&auth),
        reqwest::Client::new(),
    );
    (stream, sql)
}

fn build_batch(cols: &[ColumnSchema], rows: &[TableRow], offset: &OffsetToken) -> RowBatch {
    let mut builder = RowBatchBuilder::new();
    for row in rows {
        builder.push_row(cols, row, CdcMeta::new(CdcOperation::Insert, "0"), offset).unwrap();
    }
    builder.finish().unwrap().into_iter().next().unwrap()
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn channel_open_insert_status_drop() {
    let config = load_test_config().clone_without_credentials();
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
            .open_channel(config.database(), config.schema(), &table, &channel, None)
            .await
            .expect("open_channel failed");
        assert!(!resp.continuation_token.is_empty(), "expected non-empty continuation_token");
        assert!(resp.offset_token.is_none(), "unexpected offset on fresh channel");

        // Insert rows.
        let cols = [
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, true),
            ColumnSchema::new("name".into(), Type::TEXT, -1, 2, true),
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
                config.database(),
                config.schema(),
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
        let committed = poll_stream_offset(
            &stream,
            &config,
            &table,
            &channel,
            &offset,
            std::time::Duration::from_secs(5),
            18,
        )
        .await;
        assert_eq!(committed, Some(offset), "committed offset must match inserted offset");

        // Drop channel.
        stream
            .drop_channel(config.database(), config.schema(), &table, &channel)
            .await
            .expect("drop_channel failed");
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn channel_reopen_preserves_offset() {
    let config = load_test_config().clone_without_credentials();
    let (stream, sql) = build_clients(&config);

    let table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let channel = format!("etl_test_{}_ch0", uuid::Uuid::new_v4().simple());

    with_table_cleanup(&sql, &[&table], || async {
        sql.create_table_if_not_exists(&table, r#""id" NUMBER(10,0), "name" VARCHAR"#)
            .await
            .expect("create table failed");

        // Open and insert.
        let open_resp = stream
            .open_channel(config.database(), config.schema(), &table, &channel, None)
            .await
            .expect("open_channel failed");

        let cols = [
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, true),
            ColumnSchema::new("name".into(), Type::TEXT, -1, 2, true),
        ];
        let offset: OffsetToken = "0000000000000001/0000000000000000".parse().unwrap();
        let batch = build_batch(
            &cols,
            &[TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())])],
            &offset,
        );
        stream
            .insert_rows(
                config.database(),
                config.schema(),
                &table,
                &channel,
                &batch,
                &open_resp.continuation_token,
            )
            .await
            .expect("insert_rows failed");

        // Poll until offset is committed.
        let committed = poll_stream_offset(
            &stream,
            &config,
            &table,
            &channel,
            &offset,
            std::time::Duration::from_secs(5),
            18,
        )
        .await;
        assert_eq!(committed, Some(offset.clone()), "committed offset must match inserted offset");

        // Reopen channel, idempotent, should return the committed offset.
        let reopen_resp = stream
            .open_channel(config.database(), config.schema(), &table, &channel, None)
            .await
            .expect("reopen channel failed");
        assert_eq!(
            reopen_resp.offset_token,
            Some(offset),
            "reopened channel must return the committed offset"
        );

        let _ = stream.drop_channel(config.database(), config.schema(), &table, &channel).await;
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn channel_reopen_prevents_previous_sequencer_from_committing() {
    let config = load_test_config().clone_without_credentials();
    let (stream, sql) = build_clients(&config);

    let table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let channel = format!("etl_test_{}_ch0", uuid::Uuid::new_v4().simple());

    with_table_cleanup(&sql, &[&table], || async {
        sql.create_table_if_not_exists(&table, r#""id" NUMBER(10,0)"#)
            .await
            .expect("create table failed");

        let first_open = stream
            .open_channel(config.database(), config.schema(), &table, &channel, None)
            .await
            .expect("initial open failed");
        let second_open = stream
            .open_channel(config.database(), config.schema(), &table, &channel, None)
            .await
            .expect("reopen failed");

        let columns = [ColumnSchema::new("id".into(), Type::INT4, -1, 1, true)];
        let fenced_offset: OffsetToken = "0000000000000001/0000000000000000".parse().unwrap();
        let current_offset: OffsetToken = "0000000000000001/0000000000000001".parse().unwrap();
        let fenced_batch =
            build_batch(&columns, &[TableRow::new(vec![Cell::I32(1)])], &fenced_offset);
        let current_batch =
            build_batch(&columns, &[TableRow::new(vec![Cell::I32(2)])], &current_offset);

        // A fenced append may report a stale sequencer or be acknowledged as a no-op.
        // Only durable status and table contents prove that its row could not commit.
        match stream
            .insert_rows(
                config.database(),
                config.schema(),
                &table,
                &channel,
                &fenced_batch,
                &first_open.continuation_token,
            )
            .await
        {
            Ok(_) | Err(Error::Snowpipe(SnowpipeError::StaleContinuation)) => {}
            Err(error) => panic!("append with the fenced token failed unexpectedly: {error}"),
        }

        stream
            .insert_rows(
                config.database(),
                config.schema(),
                &table,
                &channel,
                &current_batch,
                &second_open.continuation_token,
            )
            .await
            .expect("append with the current token failed");

        let committed = poll_stream_offset(
            &stream,
            &config,
            &table,
            &channel,
            &current_offset,
            Duration::from_secs(5),
            18,
        )
        .await;
        assert_eq!(committed, Some(current_offset));

        // A successful drop proves no submitted batch remains in flight.
        stream
            .drop_channel(config.database(), config.schema(), &table, &channel)
            .await
            .expect("drop channel failed");

        let fqn = format!("\"{}\".\"{}\".\"{table}\"", config.database(), config.schema());
        let rows = query_rows(&sql, &format!("select \"id\" from {fqn}"))
            .await
            .expect("query rows failed");
        assert_eq!(
            rows,
            vec![vec![serde_json::json!("2")]],
            "only the current sequencer's row must commit"
        );
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn channel_reopen_can_rewind_offset() {
    /// Waits for an offset and its row counters to reach the expected values.
    async fn wait_for_progress(
        stream: &RestStreamClient<AuthManager<HttpExchanger>>,
        config: &Config,
        table: &str,
        channel: &str,
        expected_offset: &OffsetToken,
        expected_rows_inserted: u64,
        expected_rows_parsed: u64,
    ) -> (u64, u64, u64) {
        let mut last_status = None;
        for _ in 0..18 {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let status = stream
                .channel_status(config.database(), config.schema(), table, channel)
                .await
                .expect("channel status failed");
            if status.offset_token.as_ref() == Some(expected_offset)
                && status.rows_inserted >= expected_rows_inserted
                && status.rows_parsed >= expected_rows_parsed
            {
                return (status.rows_inserted, status.rows_parsed, status.rows_error_count);
            }
            last_status = Some(status);
        }

        panic!("channel progress did not converge within timeout; last status: {last_status:?}");
    }

    let config = load_test_config().clone_without_credentials();
    let (stream, sql) = build_clients(&config);

    let table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let channel = format!("etl_test_{}_ch0", uuid::Uuid::new_v4().simple());

    with_table_cleanup(&sql, &[&table], || async {
        sql.create_table_if_not_exists(&table, r#""id" NUMBER(10,0), "name" VARCHAR"#)
            .await
            .expect("create table failed");

        let cols = [
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, true),
            ColumnSchema::new("name".into(), Type::TEXT, -1, 2, true),
        ];
        let truncate_offset: OffsetToken = "0000000000000001/0000000000000000".parse().unwrap();
        let post_truncate_offset: OffsetToken =
            "0000000000000001/0000000000000001".parse().unwrap();
        let initial_batch = build_batch(
            &cols,
            &[TableRow::new(vec![Cell::I32(1), Cell::String("initial".into())])],
            &post_truncate_offset,
        );

        let open = stream
            .open_channel(config.database(), config.schema(), &table, &channel, None)
            .await
            .expect("open channel failed");
        let created_on_ms = open.status.created_on_ms.expect("open response missing created_on_ms");
        stream
            .insert_rows(
                config.database(),
                config.schema(),
                &table,
                &channel,
                &initial_batch,
                &open.continuation_token,
            )
            .await
            .expect("initial append failed");
        let (initial_rows_inserted, initial_rows_parsed, initial_rows_error_count) =
            wait_for_progress(&stream, &config, &table, &channel, &post_truncate_offset, 1, 1)
                .await;
        assert_eq!(initial_rows_inserted, 1);
        assert_eq!(initial_rows_parsed, 1);
        assert_eq!(initial_rows_error_count, 0);

        let rewound = stream
            .open_channel(
                config.database(),
                config.schema(),
                &table,
                &channel,
                Some(&truncate_offset),
            )
            .await
            .expect("reopen with earlier offset failed");
        assert_eq!(
            rewound.offset_token.as_ref(),
            Some(&truncate_offset),
            "reopen must move the committed offset to the requested boundary"
        );
        assert_eq!(
            rewound.status.created_on_ms,
            Some(created_on_ms),
            "explicit rewind must preserve the observed creation timestamp"
        );
        assert_eq!(
            rewound.status.rows_inserted, initial_rows_inserted,
            "explicit rewind must preserve the inserted-row counter"
        );
        assert_eq!(
            rewound.status.rows_parsed, initial_rows_parsed,
            "explicit rewind must preserve the parsed-row counter"
        );
        assert_eq!(
            rewound.status.rows_error_count, initial_rows_error_count,
            "explicit rewind must preserve the rejected-row counter"
        );

        let replay_batch = build_batch(
            &cols,
            &[TableRow::new(vec![Cell::I32(2), Cell::String("replayed".into())])],
            &post_truncate_offset,
        );
        stream
            .insert_rows(
                config.database(),
                config.schema(),
                &table,
                &channel,
                &replay_batch,
                &rewound.continuation_token,
            )
            .await
            .expect("append after offset rewind failed");

        let (final_rows_inserted, final_rows_parsed, final_rows_error_count) = wait_for_progress(
            &stream,
            &config,
            &table,
            &channel,
            &post_truncate_offset,
            initial_rows_inserted + 1,
            initial_rows_parsed + 1,
        )
        .await;
        assert_eq!(final_rows_inserted, initial_rows_inserted + 1);
        assert_eq!(final_rows_parsed, initial_rows_parsed + 1);
        assert_eq!(final_rows_error_count, 0);

        stream
            .drop_channel(config.database(), config.schema(), &table, &channel)
            .await
            .expect("drop channel failed");
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn continuation_token() {
    let config = load_test_config().clone_without_credentials();
    let (stream, sql) = build_clients(&config);

    let table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let channel = format!("etl_test_{}_ch0", uuid::Uuid::new_v4().simple());

    with_table_cleanup(&sql, &[&table], || async {
        sql.create_table_if_not_exists(&table, r#""id" NUMBER(10,0), "name" VARCHAR"#)
            .await
            .expect("create table failed");

        let fqn = format!("\"{}\".\"{}\".\"{table}\"", config.database(), config.schema());

        /// Waits for a stream offset without using a virtual warehouse.
        async fn wait_for_offset(
            stream: &RestStreamClient<AuthManager<HttpExchanger>>,
            config: &Config,
            table: &str,
            channel: &str,
            expected_offset: &OffsetToken,
        ) {
            let committed = poll_stream_offset(
                stream,
                config,
                table,
                channel,
                expected_offset,
                Duration::from_secs(5),
                18,
            )
            .await;
            assert_eq!(
                committed,
                Some(expected_offset.clone()),
                "expected offset not committed within timeout"
            );
        }

        /// Checks the materialized row count after an offset is durable.
        async fn assert_row_count(
            sql: &SqlClient<AuthManager<HttpExchanger>>,
            fqn: &str,
            expected_rows: usize,
        ) {
            let rows = query_rows(sql, &format!("select * from {fqn} order by \"id\""))
                .await
                .expect("query_rows failed");
            assert_eq!(rows.len(), expected_rows, "unexpected row count: {rows:?}");
        }

        let resp = stream
            .open_channel(config.database(), config.schema(), &table, &channel, None)
            .await
            .expect("open_channel failed");

        let cols = [
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, true),
            ColumnSchema::new("name".into(), Type::TEXT, -1, 2, true),
        ];

        // Batch 1
        let offset1: OffsetToken = "0000000000000001/0000000000000000".parse().unwrap();
        let batch1 = build_batch(
            &cols,
            &[TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())])],
            &offset1,
        );
        let insert1 = stream
            .insert_rows(
                config.database(),
                config.schema(),
                &table,
                &channel,
                &batch1,
                &resp.continuation_token,
            )
            .await
            .expect("insert batch 1 failed");

        wait_for_offset(&stream, &config, &table, &channel, &offset1).await;

        // Batch 2: uses continuation_token from batch 1.
        let offset2: OffsetToken = "0000000000000001/0000000000000001".parse().unwrap();
        let batch2 = build_batch(
            &cols,
            &[TableRow::new(vec![Cell::I32(2), Cell::String("Bob".into())])],
            &offset2,
        );
        let insert2 = stream
            .insert_rows(
                config.database(),
                config.schema(),
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
        wait_for_offset(&stream, &config, &table, &channel, &offset2).await;

        // Batch 3: use STALE token from batch 1 (already consumed by batch 2).
        let offset3: OffsetToken = "0000000000000001/0000000000000002".parse().unwrap();
        let batch3 = build_batch(
            &cols,
            &[TableRow::new(vec![Cell::I32(3), Cell::String("Charlie".into())])],
            &offset3,
        );
        let insert3 = stream
            .insert_rows(
                config.database(),
                config.schema(),
                &table,
                &channel,
                &batch3,
                &insert1.continuation_token,
            )
            .await
            .expect("insert with stale token should not error");

        // Same continuation token returned.
        assert_eq!(
            insert3.continuation_token, insert2.continuation_token,
            "stale token should not advance the sequencer"
        );
        // Still 2 rows, offset token is not advanced to offset3.
        assert_row_count(&sql, &fqn, 2).await;

        // Retry batch 3 with the CORRECT token, data commits, offset is updated.
        let insert3_retry = stream
            .insert_rows(
                config.database(),
                config.schema(),
                &table,
                &channel,
                &batch3,
                &insert2.continuation_token,
            )
            .await
            .expect("insert batch 3 with correct token failed");

        assert_ne!(
            insert3_retry.continuation_token, insert2.continuation_token,
            "correct token should advance the sequencer"
        );
        wait_for_offset(&stream, &config, &table, &channel, &offset3).await;
        assert_row_count(&sql, &fqn, 3).await;

        // Cleanup
        let _ = stream.drop_channel(config.database(), config.schema(), &table, &channel).await;
    })
    .await;
}
