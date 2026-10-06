//! Credentialed tests for request packing and large rows (PIPE-1170).
//!
//! These run through the production encoder and destination. They pin the
//! measured Snowflake request limit, prove that multi-frame request bodies
//! land every row, carry rows far above the old 2 MiB cap through copy and
//! CDC, and show that a rejected row never acknowledges unsent data.

use std::{
    error::Error as _,
    fmt::Write as _,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use etl::{
    data::{Cell, TableRow},
    destination::{DestinationWriteStatus, WriteEventsDurability},
    error::ErrorKind,
    event::{Event, EventType, InsertEvent},
    pipeline::PipelineId,
    schema::{ColumnSchema, PgLsn, ReplicatedTableSchema, TableId, TableName, TableSchema, Type},
    store::{SchemaStore, StateStore, TableStateType, WorkerType},
    test_utils::{
        database::{spawn_source_database, test_table_name},
        destination::{write_events, write_table_rows},
        event::EventCondition,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
    },
};
use etl_destinations::snowflake::{
    AuthManager, CdcMeta, CdcOperation, ChannelStatusResponse, Client, Destination, Error,
    HttpExchanger, InsertRowsResponse, OffsetToken, OpenChannelResponse, RestStreamClient, Result,
    RowBatch, RowBatchBuilder, SnowpipeError, SqlClient, StreamClient,
    test_utils::{load_test_config, query_rows},
};
use sha2::{Digest, Sha256};

use super::common::{build_auth, poll_destination_offset, poll_stream_offset, with_table_cleanup};

/// Measured Snowflake request limit, in bytes.
const REQUEST_LIMIT_BYTES: usize = 4 * 1024 * 1024;
/// Serialized size of the production row that motivated this work.
const PRODUCTION_ROW_BYTES: usize = 13_600_000;
const SIXTEEN_MIB: usize = 16 * 1024 * 1024;
const OFFSET_POLL_INTERVAL: Duration = Duration::from_secs(2);
const OFFSET_MAX_ATTEMPTS: usize = 90;
/// Deadline for source replication and destination durability in the pipeline
/// test.
const PIPELINE_PROGRESS_TIMEOUT: Duration = Duration::from_secs(180);

/// Counts append requests on the way to the real REST client.
struct CountingStreamClient {
    inner: RestStreamClient<AuthManager<HttpExchanger>>,
    inserts: AtomicUsize,
}

impl StreamClient for CountingStreamClient {
    async fn discover_ingest_host(&self) -> Result<String> {
        self.inner.discover_ingest_host().await
    }

    async fn open_channel(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        channel: &str,
        offset_token: Option<&OffsetToken>,
    ) -> Result<OpenChannelResponse> {
        self.inner.open_channel(database, schema, table, channel, offset_token).await
    }

    async fn drop_channel(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        channel: &str,
    ) -> Result<()> {
        self.inner.drop_channel(database, schema, table, channel).await
    }

    async fn insert_rows(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        channel: &str,
        batch: &RowBatch,
        continuation_token: &str,
    ) -> Result<InsertRowsResponse> {
        self.inserts.fetch_add(1, Ordering::SeqCst);
        self.inner.insert_rows(database, schema, table, channel, batch, continuation_token).await
    }

    async fn channel_status(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        channel: &str,
    ) -> Result<ChannelStatusResponse> {
        self.inner.channel_status(database, schema, table, channel).await
    }
}

type LargeRowDestination =
    Destination<NotifyingStore, AuthManager<HttpExchanger>, CountingStreamClient>;

/// Destination wired through the counting stream client, plus the pieces the
/// assertions need.
struct Harness {
    destination: LargeRowDestination,
    stream: Arc<CountingStreamClient>,
    sql: SqlClient<AuthManager<HttpExchanger>>,
    store: NotifyingStore,
    database: String,
    schema: String,
}

impl Harness {
    fn new(store: NotifyingStore) -> Self {
        Self::with_pipeline_id(store, 1)
    }

    /// Constructs a harness with a distinct identity for a real source
    /// pipeline.
    fn with_pipeline_id(store: NotifyingStore, pipeline_id: PipelineId) -> Self {
        let config = load_test_config().clone_without_credentials();
        let auth = build_auth();
        let http = reqwest::Client::new();
        let stream = Arc::new(CountingStreamClient {
            inner: RestStreamClient::new(
                config.account_url().to_owned(),
                Arc::clone(&auth),
                http.clone(),
            ),
            inserts: AtomicUsize::new(0),
        });
        let sql = SqlClient::new(config.clone_without_credentials(), Arc::clone(&auth), http);
        let client = Client::with_clients(
            SqlClient::new(config.clone_without_credentials(), auth, reqwest::Client::new()),
            Arc::clone(&stream),
            config.database().to_owned(),
            config.schema().to_owned(),
            pipeline_id,
        );
        let destination = Destination::new(client, store.clone());
        Self {
            destination,
            stream,
            sql,
            store,
            database: config.database().to_owned(),
            schema: config.schema().to_owned(),
        }
    }

    fn inserts(&self) -> usize {
        self.stream.inserts.load(Ordering::SeqCst)
    }

    /// Waits for `offset` to commit, then returns `(id, length, sha256,
    /// changed, operation)` per row ordered by sequence number and id.
    async fn committed_rows(
        &self,
        table_id: TableId,
        sf_table: &str,
        offset: &OffsetToken,
    ) -> Vec<Vec<serde_json::Value>> {
        let committed = poll_destination_offset(
            &self.destination,
            table_id,
            offset,
            OFFSET_POLL_INTERVAL,
            OFFSET_MAX_ATTEMPTS,
        )
        .await;
        assert_eq!(committed.as_ref(), Some(offset), "offset {offset} did not commit");
        let fqn = format!("\"{}\".\"{}\".\"{sf_table}\"", self.database, self.schema);
        query_rows(
            &self.sql,
            &format!(
                "select \"id\", length(\"payload\"), sha2(\"payload\", 256), \"changed\", \
                 \"_cdc_operation\" from {fqn} order by \"_cdc_sequence_number\", \"id\""
            ),
        )
        .await
        .unwrap()
    }
}

fn snowflake_table_name(src_schema: &str, src_table: &str) -> String {
    let escaped_schema = src_schema.replace('_', "__");
    let escaped_table = src_table.replace('_', "__");
    format!("{escaped_schema}_{escaped_table}").to_uppercase()
}

fn unique_source_table() -> String {
    format!("ETL_TEST_LARGE_{}", uuid::Uuid::new_v4().simple()).to_uppercase()
}

/// `id`, `payload`, `changed`: the payload column carries the large value and
/// `changed` is the small column an update touches.
fn large_row_schema(table_id: TableId, src_table: &str) -> TableSchema {
    TableSchema::new(
        table_id,
        TableName::new("public".to_owned(), src_table.to_owned()),
        vec![
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("payload".into(), Type::TEXT, -1, 2, false),
            ColumnSchema::new("changed".into(), Type::INT4, -1, 3, false),
        ],
    )
}

fn row(id: i32, payload: &str, changed: i32) -> TableRow {
    TableRow::new(vec![Cell::I32(id), Cell::String(payload.to_owned()), Cell::I32(changed)])
}

fn insert_event(schema: &ReplicatedTableSchema, lsn: u64, ordinal: u64, row: TableRow) -> Event {
    Event::Insert(InsertEvent {
        commit_lsn: PgLsn::from(lsn),
        tx_ordinal: ordinal,
        replicated_table_schema: schema.clone(),
        table_row: row,
    })
}

fn random_text(len: usize, seed: u64) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut state = seed;
    (0..len)
        .map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            ALPHABET[((state >> 33) % 62) as usize] as char
        })
        .collect()
}

/// 256-byte records, each with `random_len` random letters and fixed
/// structure. 88 random letters compress about 3.8x, 40 about 6x.
fn structured_text(len: usize, seed: u64, random_len: usize) -> String {
    let random = random_text((len / 256 + 1) * random_len, seed);
    let mut payload = String::with_capacity(len + 256);
    for chunk in random.as_bytes().chunks_exact(random_len) {
        let start = payload.len();
        payload.push_str("{\"token\":\"");
        payload.push_str(std::str::from_utf8(chunk).unwrap());
        payload.push_str("\",\"padding\":\"");
        while payload.len() < start + 253 {
            payload.push('0');
        }
        payload.push_str("\"}\n");
        if payload.len() >= len {
            break;
        }
    }
    payload.truncate(len);
    payload
}

fn sha256_hex(payload: &str) -> String {
    let mut hex = String::with_capacity(64);
    for byte in Sha256::digest(payload.as_bytes()) {
        write!(&mut hex, "{byte:02x}").unwrap();
    }
    hex
}

fn assert_stored(
    stored: &[serde_json::Value],
    id: i32,
    payload: &str,
    changed: i32,
    operation: &str,
) {
    assert_eq!(stored[0], serde_json::json!(id.to_string()));
    assert_eq!(stored[1], serde_json::json!(payload.len().to_string()));
    assert_eq!(stored[2], serde_json::json!(sha256_hex(payload)));
    assert_eq!(stored[3], serde_json::json!(changed.to_string()));
    assert_eq!(stored[4], serde_json::json!(operation));
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn request_limit_accepts_exactly_four_mebibytes_and_rejects_one_more_byte() {
    let config = load_test_config().clone_without_credentials();
    let auth = build_auth();
    let stream = RestStreamClient::new(
        config.account_url().to_owned(),
        Arc::clone(&auth),
        reqwest::Client::new(),
    );
    let sql = SqlClient::new(config.clone_without_credentials(), auth, reqwest::Client::new());
    let table = format!("ETL_TEST_LIMIT_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let channel = format!("etl_test_{}_ch0", uuid::Uuid::new_v4().simple());

    with_table_cleanup(&sql, &[&table], || async {
        sql.create_table_if_not_exists(
            &table,
            r#""id" NUMBER(10,0), "payload" VARCHAR, "_cdc_operation" VARCHAR, "_cdc_sequence_number" VARCHAR"#,
        )
        .await
        .unwrap();
        let open =
            stream.open_channel(config.database(), config.schema(), &table, &channel, None).await.unwrap();

        let cols = [
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, true),
            ColumnSchema::new("payload".into(), Type::TEXT, -1, 2, true),
        ];
        let one_row = |id: i32, offset: &OffsetToken| {
            let mut builder = RowBatchBuilder::new(TableId::new(1));
            let row = TableRow::new(vec![Cell::I32(id), Cell::String(random_text(1024 * 1024, 1))]);
            let completed = builder
                .push_row(&cols, &row, CdcMeta::new(CdcOperation::Insert, offset.as_ref()), offset)
                .unwrap();
            assert!(completed.is_empty());
            builder.finish().unwrap().into_iter().next().unwrap()
        };

        let at_limit_offset = OffsetToken::new(PgLsn::from(1_u64), 1);
        let at_limit = one_row(1, &at_limit_offset).padded_to(REQUEST_LIMIT_BYTES);
        assert_eq!(at_limit.size(), REQUEST_LIMIT_BYTES);
        let accepted = stream
            .insert_rows(
                config.database(),
                config.schema(),
                &table,
                &channel,
                &at_limit,
                &open.continuation_token,
            )
            .await
            .unwrap();

        let over_limit_offset = OffsetToken::new(PgLsn::from(1_u64), 2);
        let over_limit = one_row(2, &over_limit_offset).padded_to(REQUEST_LIMIT_BYTES + 1);
        let error = stream
            .insert_rows(
                config.database(),
                config.schema(),
                &table,
                &channel,
                &over_limit,
                &accepted.continuation_token,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                &error,
                Error::Snowpipe(SnowpipeError::HttpStatus { status })
                    if *status == reqwest::StatusCode::PAYLOAD_TOO_LARGE
            ),
            "{error:?}"
        );

        let committed = poll_stream_offset(
            &stream,
            &config,
            &table,
            &channel,
            &at_limit_offset,
            Duration::from_secs(5),
            36,
        )
        .await;
        assert_eq!(committed, Some(at_limit_offset));

        let fqn = format!("\"{}\".\"{}\".\"{table}\"", config.database(), config.schema());
        let rows = query_rows(&sql, &format!("select \"id\", length(\"payload\") from {fqn}"))
            .await
            .unwrap();
        assert_eq!(rows, vec![vec![serde_json::json!("1"), serde_json::json!("1048576")]]);

        let _ = stream.drop_channel(config.database(), config.schema(), &table, &channel).await;
    })
    .await;
}

/// Verifies that one request can combine stream frames with a row frame whose
/// repeated blocks need matches beyond level 3's default 2 MiB window.
#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn multi_frame_request_lands_every_row_in_one_append() {
    let harness = Harness::new(NotifyingStore::new());
    let src_table = unique_source_table();
    let sf_table = snowflake_table_name("public", &src_table);
    let table_id = TableId::new(1301);
    let table_schema = large_row_schema(table_id, &src_table);
    let schema = ReplicatedTableSchema::all(Arc::new(table_schema.clone()));
    harness.store.store_table_schema(table_schema).await.unwrap();

    // A small row, a row above the small-row limit, then a small row: the
    // body is a stream frame, a row frame, and a second stream frame.
    let large = random_text(3 * 1024 * 1024, 71).repeat(5);
    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        let status = write_table_rows(
            &harness.destination,
            &schema,
            vec![row(1, "small one", 0), row(2, &large, 0), row(3, "small two", 0)],
        )
        .await
        .unwrap();
        assert_eq!(status, DestinationWriteStatus::Accepted);
        assert_eq!(harness.inserts(), 1, "three frames must travel in one request");

        let barrier = write_table_rows(&harness.destination, &schema, vec![]).await.unwrap();
        assert_eq!(barrier, DestinationWriteStatus::Durable);

        let copy_offset = OffsetToken::new(PgLsn::from(0_u64), 1);
        let rows = harness.committed_rows(table_id, &sf_table, &copy_offset).await;
        assert_eq!(rows.len(), 3);
        assert_stored(&rows[0], 1, "small one", 0, "insert");
        assert_stored(&rows[1], 2, &large, 0, "insert");
        assert_stored(&rows[2], 3, "small two", 0, "insert");
    })
    .await;
}

/// Exercises unchanged TOAST reconstruction through PostgreSQL and verifies the
/// copied, inserted, and updated large values in Snowflake after durability.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local Postgres and Snowflake credentials"]
async fn large_rows_survive_copy_insert_and_full_replica_identity_update() {
    let database = spawn_source_database().await;
    let name = test_table_name(&unique_source_table());
    let table_id = database
        .create_table(
            name.clone(),
            true,
            &[("payload", "text not null"), ("changed", "integer not null default 0")],
        )
        .await
        .unwrap();
    let pg = database.client.as_ref().unwrap();
    pg.batch_execute(&format!(
        "alter table {} alter column payload set storage external; alter table {} replica \
         identity full",
        name.as_quoted_identifier(),
        name.as_quoted_identifier(),
    ))
    .await
    .unwrap();
    let publication = "large_row_pub";
    database.create_publication(publication, std::slice::from_ref(&name)).await.unwrap();

    let production_row = structured_text(PRODUCTION_ROW_BYTES, 21, 88);
    let sixteen_mib = structured_text(SIXTEEN_MIB, 22, 40);
    let inserted = structured_text(PRODUCTION_ROW_BYTES, 23, 88);
    pg.execute(
        &format!("insert into {} (payload) values ($1), ($2)", name.as_quoted_identifier()),
        &[&production_row, &sixteen_mib],
    )
    .await
    .unwrap();

    let pipeline_id = rand::random();
    let harness = Harness::with_pipeline_id(NotifyingStore::new(), pipeline_id);
    let sf_table = snowflake_table_name(&name.schema, &name.name);
    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        let destination = TestDestinationWrapper::wrap(harness.destination.clone());
        let mut pipeline = create_pipeline(
            &database.config,
            pipeline_id,
            publication.into(),
            harness.store.clone(),
            destination.clone(),
        );
        let copied = harness.store.notify_on_table_sync_complete(table_id).await;
        let ready = harness.store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
        pipeline.start().await.unwrap();
        copied.wait_for(PIPELINE_PROGRESS_TIMEOUT).notified().await;

        // This insert also advances the table from copy catch-up to Ready,
        // avoiding a separate warmup row and its destination requests.
        pg.execute(
            &format!("insert into {} (payload) values ($1)", name.as_quoted_identifier()),
            &[&inserted],
        )
        .await
        .unwrap();
        ready.wait_for(PIPELINE_PROGRESS_TIMEOUT).notified().await;

        let updated = destination
            .wait_for_events(vec![EventCondition::TableCount(EventType::Update, table_id, 1)])
            .await;
        pg.execute(
            &format!("update {} set changed = 1 where id = 3", name.as_quoted_identifier()),
            &[],
        )
        .await
        .unwrap();
        updated.wait_for(PIPELINE_PROGRESS_TIMEOUT).notified().await;
        let update_lsn = destination
            .get_events()
            .await
            .into_iter()
            .find_map(|event| match event {
                Event::Update(event) => Some(event.commit_lsn),
                _ => None,
            })
            .unwrap();

        // Observing an UPDATE does not prove its COMMIT was consumed. Poll the
        // local store until durable progress covers it before stopping intake;
        // this avoids extra Snowflake status or SQL queries from the test.
        let durable = tokio::time::timeout(PIPELINE_PROGRESS_TIMEOUT, async {
            loop {
                if harness
                    .store
                    .get_replication_checkpoint(WorkerType::Apply)
                    .await
                    .unwrap()
                    .is_some_and(|lsn| lsn >= update_lsn)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        pipeline.shutdown_and_wait().await.unwrap();
        durable.unwrap();

        let fqn = format!("\"{}\".\"{}\".\"{sf_table}\"", harness.database, harness.schema);
        let rows = query_rows(
            &harness.sql,
            &format!(
                "select \"id\", length(\"payload\"), sha2(\"payload\", 256), \"changed\", \
                 \"_cdc_operation\" from {fqn} order by \"id\", \"changed\""
            ),
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 4);
        assert_stored(&rows[0], 1, &production_row, 0, "insert");
        assert_stored(&rows[1], 2, &sixteen_mib, 0, "insert");
        assert_stored(&rows[2], 3, &inserted, 0, "insert");
        assert_stored(&rows[3], 3, &inserted, 1, "update");
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn rejected_row_keeps_the_accepted_prefix_and_replay_skips_committed_rows() {
    let store = NotifyingStore::new();
    let harness = Harness::new(store.clone());
    let src_table = unique_source_table();
    let sf_table = snowflake_table_name("public", &src_table);
    let table_id = TableId::new(1303);
    let table_schema = large_row_schema(table_id, &src_table);
    let schema = ReplicatedTableSchema::all(Arc::new(table_schema.clone()));
    store.store_table_schema(table_schema).await.unwrap();

    // Two compressible large rows cannot share a request, so the second one
    // completes and sends the first. The third row is incompressible and far
    // over the limit: it is rejected locally, and the open request holding the
    // second row is dropped unsent.
    let first = structured_text(PRODUCTION_ROW_BYTES, 31, 88);
    let second = structured_text(PRODUCTION_ROW_BYTES, 32, 88);
    let oversized = random_text(8 * 1024 * 1024, 33);
    let events = |schema: &ReplicatedTableSchema| {
        vec![
            insert_event(schema, 20, 0, row(1, &first, 0)),
            insert_event(schema, 20, 1, row(2, &second, 0)),
            insert_event(schema, 20, 2, row(3, &oversized, 0)),
        ]
    };

    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        let empty = write_table_rows(&harness.destination, &schema, vec![]).await.unwrap();
        assert_eq!(empty, DestinationWriteStatus::Durable);

        let error =
            write_events(&harness.destination, WriteEventsDurability::MayDefer, events(&schema))
                .await
                .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnsupportedValueInDestination);
        let detail = error.source().unwrap().to_string();
        assert!(detail.starts_with(&format!("Row for table {table_id}")), "{detail}");
        assert!(detail.contains("largest column payload"), "{detail}");
        assert!(!detail.contains(&oversized[..64]), "diagnostics must not carry values");
        assert_eq!(harness.inserts(), 1, "only the completed first request was sent");

        let first_offset = OffsetToken::new(PgLsn::from(20_u64), 0);
        let rows = harness.committed_rows(table_id, &sf_table, &first_offset).await;
        assert_eq!(rows.len(), 1);
        assert_stored(&rows[0], 1, &first, 0, "insert");

        // A fresh process replays the same range: the committed row is
        // skipped, the second row is encoded again, and the same row fails.
        let restarted = Harness::new(store.clone());
        let error =
            write_events(&restarted.destination, WriteEventsDurability::MayDefer, events(&schema))
                .await
                .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnsupportedValueInDestination);
        assert_eq!(restarted.inserts(), 0, "replay must not resend the committed row");
        let rows = restarted.committed_rows(table_id, &sf_table, &first_offset).await;
        assert_eq!(rows.len(), 1);

        // Once the offending row is gone from the source range, the second
        // row goes through exactly once.
        let status = write_events(
            &restarted.destination,
            WriteEventsDurability::RequireDurable,
            events(&schema).into_iter().take(2).collect(),
        )
        .await
        .unwrap();
        assert_eq!(status, DestinationWriteStatus::Durable);
        let second_offset = OffsetToken::new(PgLsn::from(20_u64), 1);
        let rows = restarted.committed_rows(table_id, &sf_table, &second_offset).await;
        assert_eq!(rows.len(), 2);
        assert_stored(&rows[0], 1, &first, 0, "insert");
        assert_stored(&rows[1], 2, &second, 0, "insert");
    })
    .await;
}
