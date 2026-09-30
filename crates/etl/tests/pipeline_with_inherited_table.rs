//! Verifies that ordinary inheritance preserves table identity during COPY and
//! CDC.

use etl::{
    data::{Cell, TableRow, UpdatedTableRow},
    event::{Event, EventType},
    schema::TableId,
    store::TableStateType,
    test_utils::{
        database::spawn_source_database, event::EventCondition,
        memory_destination::MemoryDestination, notifying_store::NotifyingStore,
        pipeline::PipelineBuilder, test_destination_wrapper::TestDestinationWrapper,
    },
};
use etl_postgres::{below_version, version::POSTGRES_15};
use etl_telemetry::tracing::init_test_tracing;

/// Extracts the fixture's integer key and text value.
fn row_values(row: &TableRow) -> (i32, String) {
    let [Cell::I32(id), Cell::String(value)] = row.values() else {
        panic!("Fixture rows must contain an integer and text");
    };
    (*id, value.clone())
}

/// Sorts fixture rows for order-independent assertions.
fn sorted_rows(rows: &[TableRow]) -> Vec<(i32, String)> {
    let mut values = rows.iter().map(row_values).collect::<Vec<_>>();
    values.sort();
    values
}

/// Checks that inherited child rows are copied and streamed only under their
/// own identity.
async fn assert_inheritance_copy_scope(
    only_parent: bool,
    publish_via_partition_root: bool,
    max_copy_connections: u16,
    row_filter: Option<&str>,
) {
    init_test_tracing();
    let database = spawn_source_database().await;
    if row_filter.is_some() && below_version!(database.server_version(), POSTGRES_15) {
        return;
    }
    let client = database.client.as_ref().unwrap();
    client
        .batch_execute(
            "create table test.parent (id integer primary key, value text not null);
             create table test.child (primary key (id)) inherits (test.parent);
             insert into test.parent values (-1, 'filtered-out'), (1, 'parent-before');
             insert into test.child values (2, 'child-before');",
        )
        .await
        .unwrap();
    if max_copy_connections > 1 {
        // Give the planner enough physical blocks to schedule multiple CTID
        // ranges.
        client
            .batch_execute(
                "insert into test.parent
                 select id, repeat('x', 500) from generate_series(1000, 2999) id;",
            )
            .await
            .unwrap();
        let blocks: i64 = client
            .query_one(
                "select pg_relation_size('test.parent') / current_setting('block_size')::bigint",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert!(blocks > 16);
    }
    let only = if only_parent { "only " } else { "" };
    let filter = row_filter.map_or_else(String::new, |filter| format!(" where ({filter})"));
    client
        .batch_execute(&format!(
            "create publication inheritance_test for table {only}test.parent{filter}
             with (publish_via_partition_root = {publish_via_partition_root})",
        ))
        .await
        .unwrap();
    let parent_id: TableId =
        client.query_one("select 'test.parent'::regclass::oid", &[]).await.unwrap().get(0);
    let child_id: TableId =
        client.query_one("select 'test.child'::regclass::oid", &[]).await.unwrap().get(0);
    let members = client
        .query("select relid from pg_get_publication_tables('inheritance_test')", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, TableId>(0))
        .collect::<Vec<_>>();
    assert!(members.contains(&parent_id));
    assert_eq!(members.contains(&child_id), !only_parent);
    assert_eq!(members.len(), if only_parent { 1 } else { 2 });

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        rand::random(),
        "inheritance_test".to_owned(),
        store.clone(),
        destination.clone(),
    )
    .with_max_copy_connections_per_table(max_copy_connections)
    .build();

    let mut copied = Vec::new();
    let mut ready = Vec::new();
    for table_id in &members {
        copied.push(store.notify_on_table_sync_complete(*table_id).await);
        ready.push(store.notify_on_table_state_type(*table_id, TableStateType::Ready).await);
    }
    pipeline.start().await.unwrap();
    for notify in copied {
        notify.notified().await;
    }

    let mut conditions = vec![EventCondition::TableCount(EventType::Insert, parent_id, 1)];
    if !only_parent {
        conditions.push(EventCondition::TableCount(EventType::Update, child_id, 1));
        conditions.push(EventCondition::TableCount(EventType::Insert, child_id, 1));
    }
    let changes_seen = destination.wait_for_events(conditions).await;
    // The parent insert proves decoding passed the child changes even when it
    // is unpublished.
    client
        .batch_execute(
            "begin;
             update only test.child set value = 'child-after' where id = 2;
             insert into test.child values (3, 'child-new');
             insert into test.parent values (4, 'parent-new');
             commit;",
        )
        .await
        .unwrap();
    changes_seen.notified().await;
    for notify in ready {
        notify.notified().await;
    }
    pipeline.shutdown_and_wait().await.unwrap();

    let copies = destination.get_table_rows().await;
    assert_eq!(copies.len(), members.len());
    let parent_rows = sorted_rows(copies.get(&parent_id).unwrap());
    let mut expected_parent_rows = vec![(1, "parent-before".to_owned())];
    if row_filter.is_none() {
        expected_parent_rows.insert(0, (-1, "filtered-out".to_owned()));
    }
    if max_copy_connections > 1 {
        expected_parent_rows.extend((1000..3000).map(|id| (id, "x".repeat(500))));
    }
    assert_eq!(parent_rows.len(), expected_parent_rows.len());
    for (actual, expected) in parent_rows.iter().zip(&expected_parent_rows) {
        assert_eq!(actual, expected);
    }
    if only_parent {
        assert!(!copies.contains_key(&child_id));
    } else {
        assert_eq!(
            sorted_rows(copies.get(&child_id).unwrap()),
            vec![(2, "child-before".to_owned())]
        );
    }

    let mut inserts = Vec::new();
    let mut updates = Vec::new();
    for event in destination.get_events().await {
        match event {
            Event::Insert(insert) => {
                inserts.push((insert.replicated_table_schema.id(), row_values(&insert.table_row)));
            }
            Event::Update(update) => {
                let UpdatedTableRow::Full(row) = update.updated_table_row else {
                    panic!("Small fixture text values must produce full updates");
                };
                updates.push((update.replicated_table_schema.id(), row_values(&row)));
            }
            _ => {}
        }
    }
    let mut expected_inserts = vec![(parent_id, (4, "parent-new".to_owned()))];
    if only_parent {
        assert!(updates.is_empty());
    } else {
        expected_inserts.push((child_id, (3, "child-new".to_owned())));
        assert_eq!(updates, vec![(child_id, (2, "child-after".to_owned()))]);
    }
    inserts.sort_by_key(|(id, _)| id.into_inner());
    expected_inserts.sort_by_key(|(id, _)| id.into_inner());
    assert_eq!(inserts, expected_inserts);
}

/// Partition-root publishing does not merge ordinary inheritance identities.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inherited_child_is_copied_separately_with_partition_root() {
    assert_inheritance_copy_scope(false, true, 1, None).await;
}

/// Parallel COPY preserves ordinary inheritance identities without
/// partition-root publishing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inherited_child_is_copied_separately_in_parallel() {
    assert_inheritance_copy_scope(false, false, 4, None).await;
}

/// An unpublished child must not enter the parent's initial COPY.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parent_only_publication_excludes_inherited_child() {
    assert_inheritance_copy_scope(true, true, 1, None).await;
}

/// A matching row filter must not admit an unpublished child's rows into
/// parallel COPY.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filtered_parent_only_publication_excludes_inherited_child_in_parallel() {
    assert_inheritance_copy_scope(true, true, 4, Some("id > 0")).await;
}
