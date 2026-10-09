//! Pipeline consistency, retry, and shutdown under initial-copy locks.

use std::{collections::BTreeMap, time::Duration};

use etl::{
    data::{Cell, TableRow},
    error::ErrorKind,
    event::{Event, EventType},
    schema::TableId,
    store::{StateStore, TableState},
    test_utils::{
        database::{assert_table_locks_released, spawn_source_database, wait_for_table_lock},
        event::EventCondition,
        faults::FaultyOp,
        memory_destination::MemoryDestination,
        notify::DEFAULT_NOTIFY_TIMEOUT,
        notifying_store::NotifyingStore,
        pipeline::{PipelineBuilder, create_pipeline},
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{TableSelection, insert_users_data, setup_test_database_schema},
    },
};
use etl_config::shared::BatchConfig;
use etl_postgres::tokio::test_utils::connect_to_pg_database;
use rand::random;
use tokio::time::timeout;

/// Extracts the integer key and value used by the pipeline fixtures.
fn integer_pair(row: &TableRow) -> (i32, i32) {
    let [Cell::I32(id), Cell::I32(value)] = row.values() else {
        panic!("Fixture rows must contain two integers");
    };
    (*id, *value)
}

/// Concurrent committed writes and rolled-back writes leave the initial COPY
/// unchanged, then WAL catchup converges to the complete source contents.
#[tokio::test(flavor = "multi_thread")]
async fn copy_converges_with_concurrent_writers() {
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    // Leave unclaimed CTID ranges after all four workers start, including on
    // PostgreSQL builds with 32 KiB heap blocks.
    client
        .batch_execute(
            "create table test.copied (id integer primary key, value integer not null);
             alter table test.copied replica identity full;
             insert into test.copied select id, id * 2 from generate_series(1, 10000) id;
             create publication copy_pub for table test.copied",
        )
        .await
        .unwrap();
    let table_id: TableId =
        client.query_one("select 'test.copied'::regclass::oid", &[]).await.unwrap().get(0);
    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let mut holds = Vec::new();
    for _ in 0..4 {
        holds.push(destination.hold_next(FaultyOp::WriteTableRows).await);
    }
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        random(),
        "copy_pub".to_owned(),
        store.clone(),
        destination.clone(),
    )
    .with_max_copy_connections_per_table(4)
    .with_batch_config(BatchConfig { max_bytes: 1024, ..BatchConfig::default() })
    .build();

    let copied = store.notify_on_table_sync_complete(table_id).await;
    let changes = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Insert, table_id, 100),
            EventCondition::TableCount(EventType::Update, table_id, 100),
            EventCondition::TableCount(EventType::Delete, table_id, 100),
        ])
        .await;

    pipeline.start().await.unwrap();

    for hold in &holds {
        hold.wait_reached().await;
    }

    // Spread each writer's disjoint keys across the table so writes also hit
    // unclaimed CTID ranges while all four workers pause at their first batch.
    let connection_config = &database.config;
    let writers = (0..4).map(|writer| async move {
        let (writer_client, _) = connect_to_pg_database(connection_config).await;
        let start = writer * 50 + 1;
        writer_client
            .batch_execute(&format!(
                "begin;
                 update test.copied set value = -id
                     where id in (select {start} + n * 400 from generate_series(0, 24) n);
                 delete from test.copied
                     where id in (select {start} + 25 + n * 400 from generate_series(0, 24) n);
                 insert into test.copied
                     select {start} + n * 400 + 10000, -({start} + n * 400)
                     from generate_series(0, 24) n;
                 commit"
            ))
            .await
            .unwrap();
    });
    // Copy remains paused until the writers commit. An incompatible copy lock
    // would deadlock this fixture, so bound the liveness assertion.
    timeout(Duration::from_secs(10), futures::future::join_all(writers)).await.unwrap();
    client
        .batch_execute("begin; update test.copied set value = 0 where id = 1; rollback")
        .await
        .unwrap();
    client.batch_execute("vacuum analyze test.copied").await.unwrap();
    for hold in holds {
        hold.release_ok();
    }

    copied.notified().await;
    changes.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let copies = destination.get_table_rows().await;
    let rows = &copies[&table_id];
    assert_eq!(rows.len(), 10000);
    let mut actual = rows.iter().map(integer_pair).collect::<BTreeMap<_, _>>();
    assert_eq!(actual, (1..=10000).map(|id| (id, id * 2)).collect());
    for event in destination.get_events().await {
        match event {
            Event::Insert(insert) => {
                let (id, value) = integer_pair(&insert.table_row);
                assert!(actual.insert(id, value).is_none());
            }
            Event::Update(update) => {
                let (id, value) = integer_pair(update.updated_table_row.as_full().unwrap());
                assert!(actual.insert(id, value).is_some());
            }
            Event::Delete(delete) => {
                let (id, _) =
                    integer_pair(delete.old_table_row.as_ref().unwrap().as_full().unwrap());
                assert!(actual.remove(&id).is_some());
            }
            _ => {}
        }
    }
    let expected = client
        .query("select id, value from test.copied", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, expected);
}

/// A lock conflict on a later leaf discards the partial copy, releases queued
/// DDL, and automatically recopies the resulting source from a fresh snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn copy_retries_queued_partition_ddl() {
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    client
        .batch_execute(
            "create table test.root (id integer primary key, value integer) partition by range \
             (id);
             create table test.first partition of test.root for values from (0) to (2000);
             create table test.later partition of test.root for values from (2000) to (3000);
             insert into test.first select id, id from generate_series(1, 1000) id;
             insert into test.later select id, id from generate_series(2000, 2009) id;
             create publication copy_pub for table test.root with (publish_via_partition_root = \
             true)",
        )
        .await
        .unwrap();
    let row = client
        .query_one("select 'test.root'::regclass::oid, 'test.later'::regclass::oid", &[])
        .await
        .unwrap();
    let table_id: TableId = row.get(0);
    let later_id: TableId = row.get(1);
    let (ddl_client, _) = connect_to_pg_database(&database.config).await;
    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let hold = destination.hold_next(FaultyOp::WriteTableRows).await;
    let retry_drop = destination.hold_next(FaultyOp::DropTableForCopy).await;
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        random(),
        "copy_pub".to_owned(),
        store.clone(),
        destination.clone(),
    )
    .with_max_copy_connections_per_table(1)
    .build();

    let failed = store
        .notify_on_table_state(table_id, |state| {
            matches!(state, TableState::Errored { source_err, .. }
                if source_err.kind() == ErrorKind::SourceTableCopyLockConflict)
        })
        .await;
    let copied = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    hold.wait_reached().await;

    // The larger first leaf is copied first. Queue DDL on the untouched leaf
    // before allowing the only worker to move on to it.
    let resume_copy = async {
        wait_for_table_lock(client, later_id, "AccessExclusiveLock", false).await;

        hold.release_ok();

        failed.notified().await;
    };
    let (ddl, ()) = tokio::join!(
        ddl_client.batch_execute(
            "set lock_timeout = '10s';
             begin; truncate test.later; insert into test.later values (2001, -1); commit"
        ),
        resume_copy,
    );
    ddl.unwrap();

    // Retry rolls the error state back. Before the fresh copy can proceed,
    // check that the previous attempt never recorded copy completion.
    retry_drop.wait_reached().await;

    let history = store.get_table_state_history(table_id).await;
    assert!(history.iter().all(|state| matches!(state, TableState::Init | TableState::DataSync)));
    retry_drop.release_ok();

    copied.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    assert!(destination.was_table_dropped_for_copy(table_id).await);
    let copies = destination.get_table_rows().await;
    let mut actual = copies[&table_id].iter().map(integer_pair).collect::<Vec<_>>();
    actual.sort_unstable();
    let expected = client
        .query("select id, value from test.root order by id", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

/// Shutdown interrupts a stalled copy write, including the final empty call,
/// and the incomplete copy restarts from scratch.
#[tokio::test(flavor = "multi_thread")]
async fn table_copy_shutdown_interrupts_stalled_write_and_restarts() {
    // An empty table reaches the final write; a populated table stalls a child.
    for row_count in [0, 1000] {
        let mut database = spawn_source_database().await;
        let schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
        let table_id = schema.users_schema().id;
        insert_users_data(&mut database, &schema.users_schema().name, 1..=row_count).await;
        let store = NotifyingStore::new();
        let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
        let write_hold = destination.hold_next(FaultyOp::WriteTableRows).await;
        let pipeline_id = random();
        let mut pipeline = create_pipeline(
            &database.config,
            pipeline_id,
            schema.publication_name(),
            store.clone(),
            destination.clone(),
        );

        pipeline.start().await.unwrap();

        write_hold.wait_reached().await;

        timeout(DEFAULT_NOTIFY_TIMEOUT, pipeline.shutdown_and_wait()).await.unwrap().unwrap();

        assert!(destination.shutdown_called().await);
        assert!(matches!(
            store.get_table_state(table_id).await.unwrap(),
            Some(TableState::DataSync)
        ));

        // Shutdown must release both the parent barrier and child COPY locks
        // even when a destination write has stalled.
        assert_table_locks_released(database.client.as_ref().unwrap(), &schema.users_schema().name)
            .await;

        let mut pipeline = create_pipeline(
            &database.config,
            pipeline_id,
            schema.publication_name(),
            store.clone(),
            destination.clone(),
        );

        let synced = store.notify_on_table_sync_complete(table_id).await;

        pipeline.start().await.unwrap();

        synced.notified().await;

        pipeline.shutdown_and_wait().await.unwrap();

        assert_eq!(destination.get_table_rows().await[&table_id].len(), row_count);
    }
}
