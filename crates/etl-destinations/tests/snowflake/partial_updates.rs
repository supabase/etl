use std::sync::Arc;

use etl::{
    data::{Cell, OldTableRow, PartialTableRow, TableRow, UpdatedTableRow},
    destination::{DestinationWriteStatus, WriteEventsDurability},
    error::ErrorKind,
    event::{DeleteEvent, Event, InsertEvent, RelationEvent, TruncateEvent, UpdateEvent},
    schema::{
        ColumnSchema, ReplicatedTableSchema, SnapshotId, TableId, TableName, TableSchema, Type,
    },
    store::{SchemaStore, StateStore, TableStateLifecycleStore},
    test_utils::{
        destination::{drop_table_for_copy, write_events, write_table_rows},
        notifying_store::NotifyingStore,
    },
};
use etl_destinations::snowflake::{
    Client, Destination, SqlClient, materialization_query, test_utils::query_rows,
};
use serde_json::{Value, json};

use crate::snowflake::common::{build_auth, companion_query, with_table_cleanup};

/// Builds the source schema shared by the history and migration tests.
fn table_schema(id: TableId, name: String) -> TableSchema {
    TableSchema::new(
        id,
        TableName::new("public".into(), name),
        vec![
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("payload".into(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("counter".into(), Type::INT4, -1, 3, true),
        ],
    )
}

/// Decodes a JSON value returned as a string by the Snowflake SQL API.
fn parse_json_cell(value: &Value) -> Value {
    serde_json::from_str(value.as_str().unwrap()).unwrap()
}

/// Builds one complete synthetic row.
fn row(id: i32, payload: Cell, counter: i32) -> TableRow {
    TableRow::new(vec![Cell::I32(id), payload, Cell::I32(counter)])
}

/// Builds an update omitting only the unchanged payload.
fn partial(id: i32, counter: i32) -> UpdatedTableRow {
    UpdatedTableRow::Partial(PartialTableRow::new(
        3,
        TableRow::new(vec![Cell::I32(id), Cell::I32(counter)]),
        vec![1],
    ))
}

/// Verifies materialization through direct queries and a snapshot, then history
/// recovery.
#[tokio::test]
#[ignore = "requires Snowflake test credentials"]
async fn materialization_history_lifetimes_and_restart() {
    let auth = build_auth();
    let config = auth.config().clone_without_credentials();
    let sql = SqlClient::new(
        config.clone_without_credentials(),
        Arc::clone(&auth),
        reqwest::Client::new(),
    );
    let name = format!("partial{}", uuid::Uuid::new_v4().simple());
    let physical = format!("PUBLIC_{name}").to_uppercase();
    let snapshot = format!("{physical}_SNAPSHOT");
    let quote = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
    let snapshot_name =
        format!("{}.{}.{}", quote(config.database()), quote(config.schema()), quote(&snapshot));
    let table = table_schema(TableId::new(1173), name);
    let schema = ReplicatedTableSchema::all(Arc::new(table.clone()));
    let store = NotifyingStore::new();
    store.store_table_schema(table).await.unwrap();
    let destination = Destination::new(Client::new(Arc::clone(&auth), 1173), store.clone());
    with_table_cleanup(&sql, &[&snapshot, &physical], || async {
        let rows = (1..=4).map(|id| row(id, Cell::String("original".into()), 0)).collect();
        assert_eq!(
            write_table_rows(&destination, &schema, rows).await.unwrap(),
            DestinationWriteStatus::Accepted
        );
        assert_eq!(
            write_table_rows(&destination, &schema, vec![]).await.unwrap(),
            DestinationWriteStatus::Durable
        );
        let update = |ordinal, updated_table_row| {
            Event::Update(UpdateEvent {
                commit_lsn: 10_u64.into(),
                tx_ordinal: ordinal,
                replicated_table_schema: schema.clone(),
                updated_table_row,
                old_table_row: None,
            })
        };
        let delete = |ordinal, id| {
            Event::Delete(DeleteEvent {
                commit_lsn: 10_u64.into(),
                tx_ordinal: ordinal,
                replicated_table_schema: schema.clone(),
                old_table_row: Some(OldTableRow::Key(TableRow::new(vec![Cell::I32(id)]))),
            })
        };
        let insert = |ordinal, id, value: &str| {
            Event::Insert(InsertEvent {
                commit_lsn: 10_u64.into(),
                tx_ordinal: ordinal,
                replicated_table_schema: schema.clone(),
                table_row: row(id, Cell::String(value.into()), 0),
            })
        };
        let events = vec![
            update(1, UpdatedTableRow::Full(row(2, Cell::Null, 1))),
            update(2, partial(1, 1)),
            update(3, partial(2, 2)),
            delete(4, 3),
            insert(5, 3, "replacement"),
            update(6, partial(3, 3)),
            delete(7, 4),
            update(8, partial(5, 5)),
            update(9, UpdatedTableRow::Full(row(6, Cell::String("unanchored".into()), 6))),
            insert(10, 7, "old lifetime"),
            insert(11, 7, "new lifetime"),
            update(12, partial(7, 7)),
        ];
        let query = companion_query(&config, &physical);
        assert_eq!(
            write_events(&destination, WriteEventsDurability::RequireDurable, events.clone(),)
                .await
                .unwrap(),
            DestinationWriteStatus::Durable
        );
        // Persist the generated query after mixed complete and partial updates.
        sql.execute_ddl(&format!("create transient table {snapshot_name} as {query}"))
            .await
            .unwrap();
        let records = query_rows(&sql, &format!("select * from {snapshot_name} order by \"id\""))
            .await
            .unwrap();
        assert_eq!(records.len(), 6);
        let expected = [
            ("1", json!("original"), "1", true, json!([])),
            ("2", json!(null), "2", true, json!([])),
            ("3", json!("replacement"), "3", true, json!([])),
            ("5", json!(null), "5", false, json!(["payload"])),
            ("6", json!("unanchored"), "6", false, json!([])),
            ("7", json!("new lifetime"), "7", true, json!([])),
        ];
        for (record, (id, value, counter, anchored, missing)) in records.iter().zip(expected) {
            assert_eq!(&record[..3], &[json!(id), value, json!(counter)]);
            assert_eq!(record[3].as_str().unwrap().parse::<bool>().unwrap(), anchored);
            assert_eq!(parse_json_cell(&record[4]), missing);
        }
        // Reopening validates physical metadata and skips identically replayed
        // offsets.
        let restarted = Destination::new(Client::new(Arc::clone(&auth), 1173), store.clone());
        write_events(&restarted, WriteEventsDurability::RequireDurable, events).await.unwrap();
        assert_eq!(query_rows(&sql, &format!("{query} order by \"id\"")).await.unwrap(), records);
        let key_change = Event::Update(UpdateEvent {
            commit_lsn: 20_u64.into(),
            tx_ordinal: 1,
            replicated_table_schema: schema.clone(),
            updated_table_row: partial(8, 0),
            old_table_row: Some(OldTableRow::Key(TableRow::new(vec![Cell::I32(1)]))),
        });
        let error =
            write_events(&restarted, WriteEventsDurability::RequireDurable, vec![key_change])
                .await
                .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
        assert_eq!(query_rows(&sql, &format!("{query} order by \"id\"")).await.unwrap(), records);
        // A default-only relation advances durable metadata without changing
        // the interpretation of existing supplied values.
        let next_snapshot = SnapshotId::new(30_u64.into(), 31_u64.into());
        let mut columns = schema.inner().column_schemas.clone();
        columns[1] = columns[1].clone().with_default_expression("'new default'::text".into());
        let next_table = TableSchema::with_snapshot_id(
            schema.id(),
            schema.name().clone(),
            columns,
            next_snapshot,
        );
        store.store_table_schema(next_table.clone()).await.unwrap();
        let next_schema = ReplicatedTableSchema::all(Arc::new(next_table));
        write_events(
            &restarted,
            WriteEventsDurability::RequireDurable,
            vec![
                Event::Relation(RelationEvent { replicated_table_schema: next_schema.clone() }),
                Event::Update(UpdateEvent {
                    commit_lsn: 40_u64.into(),
                    tx_ordinal: 1,
                    replicated_table_schema: next_schema.clone(),
                    updated_table_row: partial(1, 99),
                    old_table_row: None,
                }),
            ],
        )
        .await
        .unwrap();
        let metadata = store.get_destination_table_metadata(schema.id()).await.unwrap().unwrap();
        assert!(metadata.is_applied());
        assert_eq!(metadata.snapshot_id(), next_snapshot);
        let current = query_rows(&sql, &format!("{query} order by \"id\"")).await.unwrap();
        assert_eq!(&current[0][..3], &[json!("1"), json!("original"), json!("99")]);
        // A truncate ends every prior lifetime; replay remains idempotent.
        let truncated = vec![
            Event::Truncate(TruncateEvent {
                commit_lsn: 50_u64.into(),
                tx_ordinal: 1,
                options: 0,
                truncated_tables: vec![next_schema.clone()],
            }),
            Event::Insert(InsertEvent {
                commit_lsn: 50_u64.into(),
                tx_ordinal: 2,
                replicated_table_schema: next_schema.clone(),
                table_row: row(1, Cell::String("after truncate".into()), 0),
            }),
            Event::Update(UpdateEvent {
                commit_lsn: 50_u64.into(),
                tx_ordinal: 3,
                replicated_table_schema: next_schema.clone(),
                updated_table_row: partial(1, 1),
                old_table_row: None,
            }),
        ];
        for _ in 0..2 {
            write_events(&restarted, WriteEventsDurability::RequireDurable, truncated.clone())
                .await
                .unwrap();
            let current = query_rows(&sql, &query).await.unwrap();
            assert_eq!(current.len(), 1);
            assert_eq!(&current[0][..3], &[json!("1"), json!("after truncate"), json!("1")]);
            assert_eq!(current[0][3], "true");
            assert_eq!(parse_json_cell(&current[0][4]), json!([]));
        }

        // The existing resync sequence discards the old generation. Reopening
        // above preserved history; only explicit reset and fresh copy replace
        // it.
        store.reset_table_states_for_resync().await.unwrap();
        drop_table_for_copy(&restarted, &next_schema).await.unwrap();
        store.prepare_table_state_for_copy(schema.id()).await.unwrap();
        store.store_table_schema(schema.inner().clone()).await.unwrap();
        write_table_rows(
            &restarted,
            &schema,
            vec![row(9, Cell::String("fresh snapshot".into()), 0)],
        )
        .await
        .unwrap();
        write_table_rows(&restarted, &schema, vec![]).await.unwrap();
        let current = query_rows(&sql, &query).await.unwrap();
        assert_eq!(current.len(), 1);
        assert_eq!(&current[0][..3], &[json!("9"), json!("fresh snapshot"), json!("0")]);
        assert_eq!(current[0][3], "true");
        assert_eq!(parse_json_cell(&current[0][4]), json!([]));
    })
    .await;
}

/// Verifies the same source workload with partial DEFAULT and complete FULL
/// tuples against the same Snowflake writer and materialization query.
async fn postgres_external_toast(full_identity: bool) {
    etl_telemetry::tracing::init_test_tracing();
    use etl::{
        event::EventType,
        store::TableStateType,
        test_utils::{
            database::{spawn_source_database, test_table_name},
            event::EventCondition,
            pipeline::create_pipeline,
            test_destination_wrapper::TestDestinationWrapper,
        },
    };
    let database = spawn_source_database().await;
    let name = test_table_name(&format!("partial{}", uuid::Uuid::new_v4().simple()));
    let table_id = database
        .create_table(
            name.clone(),
            false,
            &[
                ("id", "bigserial"),
                ("discarded", "integer"),
                ("payload", "text"),
                ("counter", "integer"),
                ("key_numeric", "numeric(40,9) not null default 12345678901234567890.123456789"),
                ("key_uuid", "uuid not null default '00000000-0000-0000-0000-000000000001'"),
                ("key_bytes", r"bytea not null default '\xdeadbeef'::bytea"),
                ("key_date", "date not null default '2026-01-01'"),
                ("key_time", "timestamptz not null default '2026-01-01 05:00:00+05'"),
                ("primary key", "(id, key_numeric, key_uuid, key_bytes, key_date, key_time)"),
            ],
        )
        .await
        .unwrap();
    let source = name.as_quoted_identifier();
    for command in [
        format!("alter table {source} drop column discarded"),
        format!(
            "alter table {source} replica identity {}",
            if full_identity { "full" } else { "default" }
        ),
        format!("alter table {source} alter column payload set storage external"),
        format!(
            "insert into {source} (payload, counter) values (repeat('x', 65536), 0), (repeat('x', \
             65536), 0)"
        ),
    ] {
        database.run_sql(&command).await.unwrap();
    }
    database.create_publication("partial_toast", std::slice::from_ref(&name)).await.unwrap();
    let auth = build_auth();
    let config = auth.config().clone_without_credentials();
    let sql = SqlClient::new(
        config.clone_without_credentials(),
        Arc::clone(&auth),
        reqwest::Client::new(),
    );
    let physical = format!("{}_{}", name.schema.replace('_', "__"), name.name.replace('_', "__"))
        .to_uppercase();
    with_table_cleanup(&sql, &[&physical], || async {
        let store = NotifyingStore::new();
        let pipeline_id = rand::random();
        let raw_destination =
            Destination::new(Client::new(Arc::clone(&auth), pipeline_id), store.clone());
        let polling = raw_destination.clone();
        let destination = TestDestinationWrapper::wrap(raw_destination);
        let mut pipeline = create_pipeline(
            &database.config,
            pipeline_id,
            "partial_toast".into(),
            store.clone(),
            destination.clone(),
        );
        let copied = store.notify_on_table_sync_complete(table_id).await;
        pipeline.start().await.unwrap();
        copied.notified().await;
        let changed = destination
            .wait_for_events(vec![EventCondition::TableCount(EventType::Update, table_id, 5)])
            .await;
        for change in [
            "counter = 1 where id = 1",
            "payload = repeat('y', 32768) where id = 1",
            "counter = 2 where id = 1",
            "payload = null where id = 2",
            "counter = 1 where id = 2",
        ] {
            database.run_sql(&format!("update {source} set {change}")).await.unwrap();
        }
        changed.notified().await;
        pipeline.shutdown_and_wait().await.unwrap();
        let events = destination.get_events().await;
        let updates = events
            .iter()
            .filter_map(|event| if let Event::Update(update) = event { Some(update) } else { None })
            .collect::<Vec<_>>();
        assert_eq!(updates.len(), 5);
        let last = updates.last().unwrap();
        let expected =
            etl_destinations::snowflake::OffsetToken::new(last.commit_lsn, last.tx_ordinal);
        assert_eq!(
            crate::snowflake::common::poll_destination_offset(
                &polling,
                table_id,
                &expected,
                std::time::Duration::from_secs(1),
                90
            )
            .await,
            Some(expected)
        );
        for index in [0, 2] {
            if full_identity {
                assert!(matches!(updates[index].old_table_row, Some(OldTableRow::Full(_))));
                assert!(matches!(updates[index].updated_table_row, UpdatedTableRow::Full(_)));
            } else {
                assert!(updates[index].old_table_row.is_none());
                let UpdatedTableRow::Partial(row) = &updates[index].updated_table_row else {
                    panic!("external unchanged payload must remain partial")
                };
                assert_eq!(row.missing_column_indexes(), &[1]);
                assert_eq!(row.values().len(), 7);
                assert!(row.values().iter().all(|cell| !matches!(cell, Cell::String(_))));
            }
        }
        let UpdatedTableRow::Full(changed) = &updates[1].updated_table_row else {
            panic!("changed payload must be supplied")
        };
        assert_eq!(changed.values()[1], Cell::String("y".repeat(32768)));
        let UpdatedTableRow::Full(cleared) = &updates[3].updated_table_row else {
            panic!("explicit null must be supplied")
        };
        assert_eq!(cleared.values()[1], Cell::Null);
        let query = companion_query(&config, &physical);
        let rows = query_rows(&sql, &format!("{query} order by \"id\"")).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(&rows[0][..3], &[json!("1"), json!("y".repeat(32768)), json!("2")]);
        assert_eq!(&rows[1][..3], &[json!("2"), json!(null), json!("1")]);
        for row in rows {
            assert_eq!(row[8], "true");
            assert_eq!(parse_json_cell(&row[9]), json!([]));
        }
        // The physical attribute number differs from the replicated row index
        // because a column was dropped before the initial snapshot.
        assert_eq!(
            updates[0].replicated_table_schema.column_schemas().nth(1).unwrap().ordinal_position,
            3
        );
        let fqn = format!("\"{}\".\"{}\".\"{physical}\"", config.database(), config.schema());
        if !full_identity {
            assert_eq!(
                query_rows(
                    &sql,
                    &format!(
                        "select count(*) from {fqn} where array_contains(3::variant, \
                         \"_cdc_missing_columns\")"
                    )
                )
                .await
                .unwrap(),
                vec![vec![json!("2")]],
            );
        }
        // Restart at each boundary. Each phase uses real transactional DDL and
        // pgoutput updates, and checks values after durability and shutdown.
        // Only two source rows are needed to distinguish omission from NULL.
        let mut previous_lsn = last.commit_lsn;
        for (phase, (count, commands, columns, expected)) in [
            (
                2_usize,
                format!(
                    "alter table {source} rename column id to record_id;
                    alter table {source} rename column payload to details;
                    alter table {source} rename column counter to scratch;
                    update {source} set scratch = scratch + 1"
                ),
                "\"record_id\", \"details\", \"scratch\"",
                vec![
                    vec![json!("1"), json!("y".repeat(32768)), json!("3")],
                    vec![json!("2"), json!(null), json!("2")],
                ],
            ),
            (
                4_usize,
                format!(
                    "alter table {source} drop column scratch;
                    alter table {source} add column scratch text default 'fresh';
                    alter table {source} alter column scratch set storage external;
                    update {source} set scratch = repeat('z', 4096) where record_id = 1;
                    update {source} set scratch = null where record_id = 2;
                    alter table {source} rename column details to temporary;
                    alter table {source} rename column scratch to details;
                    alter table {source} rename column temporary to scratch;
                    update {source} set record_id = record_id"
                ),
                "\"record_id\", \"scratch\", \"details\"",
                vec![
                    vec![json!("1"), json!("y".repeat(32768)), json!("z".repeat(4096))],
                    vec![json!("2"), json!(null), json!(null)],
                ],
            ),
            (
                2_usize,
                format!(
                    "alter table {source} drop column scratch;
                    alter table {source} add column scratch text default 'fresh';
                    alter table {source} add column added_null text;
                    update {source} set record_id = record_id"
                ),
                "\"record_id\", \"details\", \"scratch\", \"added_null\"",
                vec![
                    vec![json!("1"), json!("z".repeat(4096)), json!("fresh"), json!(null)],
                    vec![json!("2"), json!(null), json!("fresh"), json!(null)],
                ],
            ),
        ].into_iter().enumerate() {
            let destination = TestDestinationWrapper::wrap(Destination::new(
                Client::new(Arc::clone(&auth), pipeline_id),
                store.clone(),
            ));
            let mut pipeline = create_pipeline(
                &database.config,
                pipeline_id,
                "partial_toast".into(),
                store.clone(),
                destination.clone(),
            );
            // Restart can redeliver committed history. Only new transactions
            // count toward this phase; the destination must skip replayed writes.
            let changed = destination
                .notify_on_events(move |events| {
                    events.iter().filter(|event| matches!(event,
                        Event::Update(update) if update.commit_lsn > previous_lsn
                    )).count() >= count
                })
                .await;
            let failed = store.notify_on_table_state_type(table_id, TableStateType::Errored).await;
            pipeline.start().await.unwrap();
            database
                .client
                .as_ref()
                .unwrap()
                .batch_execute(&format!("begin; {commands}; commit"))
                .await
                .unwrap();
            let completion = pipeline.wait();
            tokio::pin!(completion);
            let completed = tokio::select! {
                _ = changed.notified() => true,

                _ = failed.notified() => false,

                result = &mut completion => panic!("Pipeline finished before the DDL updates: {result:?}"),

                // The column-swap phase crosses two durability barriers plus
                // two view/DDL transitions; 60 seconds is insufficient.
                _ = tokio::time::sleep(std::time::Duration::from_secs(120)) => false,
            };
            pipeline.shutdown();
            completion.await.unwrap();
            assert!(completed, "phase {phase}, state: {:?}, events: {}", store.get_table_state(table_id).await.unwrap(), destination.get_events().await.len());
            let events = destination.get_events().await;
            let updates = events
                .iter()
                .filter_map(|event| match event {
                    Event::Update(update) if update.commit_lsn > previous_lsn => Some(update),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(updates.len(), count);
            previous_lsn = updates.last().unwrap().commit_lsn;
            assert!(
                updates
                    .iter()
                    .any(|update| matches!(update.updated_table_row, UpdatedTableRow::Partial(_)))
                    != full_identity
            );
            let query = companion_query(&config, &physical);
            assert_eq!(
                query_rows(
                    &sql,
                    &format!("select {columns} from ({query}) order by \"record_id\"")
                )
                .await
                .unwrap(),
                expected
            );
            assert_eq!(
                query_rows(
                    &sql,
                    &format!(
                        "select count_if(not \"_cdc_anchored\" or \
                         array_size(\"_cdc_unresolved_columns\") > 0) from ({query})"
                    )
                )
                .await
                .unwrap(),
                vec![vec![json!("0")]]
            );
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires PostgreSQL and Snowflake test credentials"]
async fn default_postgres_external_toast() {
    postgres_external_toast(false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires PostgreSQL and Snowflake test credentials"]
async fn full_postgres_external_toast() {
    postgres_external_toast(true).await;
}

/// Verifies complete-row baselines and lifetime boundaries with typed values
/// and composite keys under a non-binary physical collation.
#[tokio::test]
#[ignore = "requires Snowflake test credentials"]
async fn materialization_query_composite_keys_and_native_values() {
    let auth = build_auth();
    let config = auth.config().clone_without_credentials();
    let sql = SqlClient::new(config.clone_without_credentials(), auth, reqwest::Client::new());
    let table = format!("materialization_types{}", uuid::Uuid::new_v4().simple());
    let quote = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
    let fqn = format!("{}.{}.{}", quote(config.database()), quote(config.schema()), quote(&table));
    let source = TableSchema::new(
        TableId::new(1174),
        TableName::new("public".into(), table.clone()),
        vec![
            ColumnSchema::new("id".into(), Type::TEXT, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("shard".into(), Type::INT4, -1, 2, false).with_primary_key(2),
            ColumnSchema::new("c0\"'".into(), Type::NUMERIC, -1, 3, true),
            ColumnSchema::new("array".into(), Type::INT4_ARRAY, -1, 4, true),
            ColumnSchema::new("json".into(), Type::JSONB, -1, 5, true),
            ColumnSchema::new("date".into(), Type::DATE, -1, 6, true),
            ColumnSchema::new("time".into(), Type::TIMESTAMPTZ, -1, 7, true),
        ],
    );
    let schema = ReplicatedTableSchema::all(Arc::new(source));
    with_table_cleanup(&sql, &[&table], || async {
        sql.create_table_if_not_exists(
            &table,
            "\"id\" varchar collate 'en-ci', \"shard\" integer, \"c0\"\"'\" varchar collate \
             'en-ci', \"array\" array, \"json\" variant, \"date\" date, \"time\" timestamp_tz, \
             \"_cdc_operation\" varchar not null, \"_cdc_sequence_number\" varchar collate \
             'en-ci' not null, \"_cdc_missing_columns\" array",
        )
        .await
        .unwrap();
        // Distinct UTF-8 keys must not collapse under the physical column's
        // collation.
        // A legacy NULL mask on a complete update replaces every value. Later
        // omissions must use that baseline while retaining the older insert's
        // anchor. A complete record before a delete cannot repair a later
        // unanchored partial update.
        sql.execute_ddl(&format!(
            "insert into {fqn} select 'A', 1, '0', array_construct(), parse_json('null'), \
             '2000-01-01'::date, '2000-01-01 00:00:00 +00:00'::timestamp_tz, 'insert', '01', \
             array_construct() union all select 'a', 1, '2', array_construct(2), \
             parse_json('null'), '2026-01-02'::date, '2026-01-02 00:00:00 +00:00'::timestamp_tz, \
             'insert', '01', array_construct() union all select 'A', 2, '3', array_construct(), \
             null, null, null, 'insert', '01', array_construct() union all select 'A', 1, \
             '12345678901234567890.123456789', array_construct(1, null, 3), \
             parse_json('{{\"x\":1}}'), '2026-01-01'::date, '2026-01-01 00:00:00.123456789 \
             +05:30'::timestamp_tz, 'update', '02', null union all select 'A', 1, null, null, \
             null, null, null, 'update', '03', array_construct(3, 4, 5, 6, 7) union all select \
             'A', 2, null, null, null, null, null, 'delete', '02', array_construct() union all \
             select 'A', 2, null, null, null, null, null, 'update', '03', array_construct(3, 4, \
             5, 6, 7)"
        ))
        .await
        .unwrap();
        let query =
            materialization_query(config.database(), config.schema(), &table, &schema).unwrap();
        let rows = query_rows(
            &sql,
            &format!(
                "select \"id\", \"shard\", \"c0\"\"'\", typeof(\"array\"), typeof(\"json\"), \
                 \"date\"::varchar, to_varchar(\"time\", 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'), \
                 \"_cdc_anchored\", \"_cdc_unresolved_columns\", \"_cdc_last_sequence\", \
                 collation(\"id\"), collation(\"c0\"\"'\"), collation(\"_cdc_last_sequence\") \
                 from ({query}) order by collate(\"id\", 'utf8'), \"shard\""
            ),
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            &rows[0][..6],
            &[
                json!("A"),
                json!("1"),
                json!("12345678901234567890.123456789"),
                json!("ARRAY"),
                json!("OBJECT"),
                json!("2026-01-01")
            ]
        );
        assert_eq!(rows[1][1], "2");
        assert_eq!(rows[2][0], "a");
        assert_eq!(rows[2][4], "NULL_VALUE");
        assert!(rows[1][2..7].iter().all(Value::is_null));
        assert_eq!(rows[0][6], "2026-01-01 00:00:00.123456789 +05:30");
        for (row, (anchored, unresolved)) in rows.iter().zip([
            ("true", json!([])),
            ("false", json!(["c0\"'", "array", "json", "date", "time"])),
            ("true", json!([])),
        ]) {
            assert_eq!(row[7], anchored);
            assert_eq!(parse_json_cell(&row[8]), unresolved);
            assert_eq!(&row[10..], &[json!("en-ci"), json!("en-ci"), json!("utf8")]);
        }
        assert_eq!(
            rows.iter().map(|row| row[9].as_str().unwrap()).collect::<Vec<_>>(),
            ["03", "03", "01"]
        );

        // A supplied SQL NULL must replace older values, while a newer omitted
        // value must not replace it. JSON null remains distinct from SQL NULL.
        sql.execute_ddl(&format!(
            "insert into {fqn} select 'A', 1, null, null, parse_json('null'), null, null, \
             'update', '04', array_construct(6, 7) union all select 'A', 1, null, null, null, \
             null, null, 'update', '05', array_construct(3, 4, 5, 6, 7)"
        ))
        .await
        .unwrap();
        let current = query_rows(
            &sql,
            &format!(
                "select \"c0\"\"'\", \"array\", is_null_value(\"json\"), \"_cdc_anchored\", \
                 \"_cdc_unresolved_columns\", \"_cdc_last_sequence\" from ({query}) where \
                 collate(\"id\", 'utf8') = 'A' and \"shard\" = 1"
            ),
        )
        .await
        .unwrap();
        assert_eq!(current.len(), 1);
        assert_eq!(&current[0][..4], &[Value::Null, Value::Null, json!("true"), json!("true")]);
        assert_eq!(parse_json_cell(&current[0][4]), json!([]));
        assert_eq!(current[0][5], "05");

        // Identical replayed events may tie on sequence without changing state.
        let ordered = format!("{query} order by collate(\"id\", 'utf8'), \"shard\"");
        let expected = query_rows(&sql, &ordered).await.unwrap();
        sql.execute_ddl(&format!("insert into {fqn} select * from {fqn}")).await.unwrap();
        assert_eq!(query_rows(&sql, &ordered).await.unwrap(), expected);
        // The empty unresolved expression is valid for a key-only projection
        // too.
        let mut key_only = schema.inner().clone();
        key_only.column_schemas.retain(ColumnSchema::primary_key);
        let query = materialization_query(
            config.database(),
            config.schema(),
            &table,
            &ReplicatedTableSchema::all(Arc::new(key_only)),
        )
        .unwrap();
        assert_eq!(query_rows(&sql, &query).await.unwrap().len(), 3);
    })
    .await;
}

/// Exercises both automatic legacy upgrade and restart after the ALTER already
/// completed, retaining real channel offsets and historical NULL semantics.
#[tokio::test]
#[ignore = "requires Snowflake test credentials"]
async fn legacy_format_upgrade_preserves_history_and_offsets() {
    use etl::destination::Destination as _;

    let auth = build_auth();
    let config = auth.config().clone_without_credentials();
    let sql = SqlClient::new(
        config.clone_without_credentials(),
        Arc::clone(&auth),
        reqwest::Client::new(),
    );
    let name = format!("upgrade{}", uuid::Uuid::new_v4().simple());
    let physical = format!("PUBLIC_{name}").to_uppercase();
    let table = table_schema(TableId::new(1174), name);
    let schema = ReplicatedTableSchema::all(Arc::new(table.clone()));
    let store = NotifyingStore::new();
    store.store_table_schema(table).await.unwrap();
    let original = Destination::new(Client::new(Arc::clone(&auth), 1174), store.clone());
    with_table_cleanup(&sql, &[&physical], || async {
        write_table_rows(
            &original,
            &schema,
            vec![row(1, Cell::String("baseline".into()), 0), row(2, Cell::Null, 0)],
        )
        .await
        .unwrap();
        write_table_rows(&original, &schema, vec![]).await.unwrap();
        let full = Event::Update(UpdateEvent {
            commit_lsn: 10_u64.into(),
            tx_ordinal: 1,
            replicated_table_schema: schema.clone(),
            updated_table_row: UpdatedTableRow::Full(row(
                1,
                Cell::String("before upgrade".into()),
                1,
            )),
            old_table_row: Some(OldTableRow::Full(row(1, Cell::String("baseline".into()), 0))),
        });
        let delete = Event::Delete(DeleteEvent {
            commit_lsn: 10_u64.into(),
            tx_ordinal: 2,
            replicated_table_schema: schema.clone(),
            old_table_row: Some(OldTableRow::Key(TableRow::new(vec![Cell::I32(2)]))),
        });
        let committed_events = vec![full, delete];
        write_events(&original, WriteEventsDurability::RequireDurable, committed_events.clone())
            .await
            .unwrap();
        let before = original.fetch_committed_offset(schema.id()).await.unwrap().unwrap();
        original.shutdown().await.unwrap();
        // All value-supplying events above are complete. Removing the mask
        // here constructs the old physical format, including a key delete.
        sql.drop_column(&physical, "_cdc_missing_columns").await.unwrap();
        let metadata = store.get_destination_table_metadata(schema.id()).await.unwrap();
        let upgraded = Destination::new(Client::new(Arc::clone(&auth), 1174), store.clone());
        // Opening upgrades the table and skips both committed events.
        write_events(&upgraded, WriteEventsDurability::RequireDurable, committed_events.clone())
            .await
            .unwrap();
        assert_eq!(
            upgraded.fetch_committed_offset(schema.id()).await.unwrap(),
            Some(before.clone())
        );
        assert_eq!(store.get_destination_table_metadata(schema.id()).await.unwrap(), metadata);
        upgraded.shutdown().await.unwrap();
        // Reopen before new writes: the ARRAY column exists, historical masks
        // are still NULL, and committed events must still be skipped.
        let upgraded = Destination::new(Client::new(Arc::clone(&auth), 1174), store.clone());
        write_events(&upgraded, WriteEventsDurability::RequireDurable, committed_events)
            .await
            .unwrap();
        assert_eq!(upgraded.fetch_committed_offset(schema.id()).await.unwrap(), Some(before));
        assert_eq!(store.get_destination_table_metadata(schema.id()).await.unwrap(), metadata);
        let events = vec![
            Event::Update(UpdateEvent {
                commit_lsn: 20_u64.into(),
                tx_ordinal: 1,
                replicated_table_schema: schema.clone(),
                updated_table_row: partial(1, 2),
                old_table_row: None,
            }),
            Event::Insert(InsertEvent {
                commit_lsn: 20_u64.into(),
                tx_ordinal: 2,
                replicated_table_schema: schema.clone(),
                table_row: row(2, Cell::String("new lifetime".into()), 0),
            }),
            Event::Update(UpdateEvent {
                commit_lsn: 20_u64.into(),
                tx_ordinal: 3,
                replicated_table_schema: schema.clone(),
                updated_table_row: partial(2, 3),
                old_table_row: None,
            }),
        ];
        write_events(&upgraded, WriteEventsDurability::RequireDurable, events.clone())
            .await
            .unwrap();
        let query = companion_query(&config, &physical);
        let rows = query_rows(&sql, &format!("{query} order by \"id\"")).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(&rows[0][..3], &[json!("1"), json!("before upgrade"), json!("2")]);
        assert_eq!(&rows[1][..3], &[json!("2"), json!("new lifetime"), json!("3")]);
        upgraded.shutdown().await.unwrap();
        let restarted = Destination::new(Client::new(Arc::clone(&auth), 1174), store.clone());
        write_events(&restarted, WriteEventsDurability::RequireDurable, events).await.unwrap();
        assert_eq!(query_rows(&sql, &format!("{query} order by \"id\"")).await.unwrap(), rows);
        let fqn = format!("\"{}\".\"{}\".\"{physical}\"", config.database(), config.schema());
        assert_eq!(
            query_rows(
                &sql,
                &format!("select count(*), count_if(\"_cdc_missing_columns\" is null) from {fqn}")
            )
            .await
            .unwrap(),
            vec![vec![json!("7"), json!("4")]]
        );
        restarted.shutdown().await.unwrap();
    })
    .await;
}

/// An almost-legacy table is not an automatic repair request.
#[tokio::test]
#[ignore = "requires Snowflake test credentials"]
async fn legacy_upgrade_rejects_unrelated_schema_damage() {
    use etl::destination::Destination as _;

    let auth = build_auth();
    let config = auth.config().clone_without_credentials();
    let sql = SqlClient::new(
        config.clone_without_credentials(),
        Arc::clone(&auth),
        reqwest::Client::new(),
    );
    for damage in ["extra", "missing", "wrong_mask"] {
        let name = format!("invalid{}", uuid::Uuid::new_v4().simple());
        let physical = format!("PUBLIC_{name}").to_uppercase();
        let table = table_schema(TableId::new(1175), name);
        let schema = ReplicatedTableSchema::all(Arc::new(table.clone()));
        let store = NotifyingStore::new();
        store.store_table_schema(table).await.unwrap();
        let original = Destination::new(Client::new(Arc::clone(&auth), 1175), store.clone());
        with_table_cleanup(&sql, &[&physical], || async {
            // Rejection depends on column metadata; history preservation is
            // tested above.
            assert_eq!(
                write_table_rows(&original, &schema, vec![]).await.unwrap(),
                DestinationWriteStatus::Durable
            );
            original.shutdown().await.unwrap();
            sql.drop_column(&physical, "_cdc_missing_columns").await.unwrap();
            match damage {
                "extra" => sql.add_column(&physical, "unexpected", "VARCHAR", None).await.unwrap(),
                "missing" => sql.drop_column(&physical, "payload").await.unwrap(),
                _ => sql
                    .add_column(&physical, "_cdc_missing_columns", "VARCHAR", None)
                    .await
                    .unwrap(),
            }
            let restarted = Destination::new(Client::new(Arc::clone(&auth), 1175), store.clone());
            let event = Event::Update(UpdateEvent {
                commit_lsn: 10_u64.into(),
                tx_ordinal: 1,
                replicated_table_schema: schema.clone(),
                updated_table_row: UpdatedTableRow::Full(row(1, Cell::Null, 1)),
                old_table_row: None,
            });
            let error =
                write_events(&restarted, WriteEventsDurability::RequireDurable, vec![event])
                    .await
                    .unwrap_err();
            assert_eq!(
                error.kind(),
                if damage == "wrong_mask" {
                    ErrorKind::DestinationLayoutUnsupported
                } else {
                    ErrorKind::CorruptedTableSchema
                }
            );
            let fqn = format!("\"{}\".\"{}\".\"{physical}\"", config.database(), config.schema());
            let columns = query_rows(&sql, &format!("show columns in table {fqn}")).await.unwrap();
            let masks = columns
                .iter()
                .filter(|column| column[2] == "_cdc_missing_columns")
                .collect::<Vec<_>>();
            if damage == "wrong_mask" {
                assert_eq!(masks.len(), 1);
                assert_eq!(parse_json_cell(&masks[0][3])["type"], "TEXT");
            } else {
                assert!(masks.is_empty());
            }
            restarted.shutdown().await.unwrap();
        })
        .await;
    }
}
