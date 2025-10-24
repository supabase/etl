#![cfg(feature = "test-utils")]

use etl::destination::memory::MemoryDestination;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::event::group_events_by_type_and_table_id;
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::create_partitioned_table;
use etl::types::EventType;
use etl::types::PipelineId;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

/// Tests that initial COPY replicates all rows from a partitioned table.
/// Only the parent table is tracked, not individual child partitions.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_table_copy_replicates_existing_data() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events");
    let partition_specs = [
        ("p1", "from (1) to (100)"),
        ("p2", "from (100) to (200)"),
        ("p3", "from (200) to (300)"),
    ];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150), ('event3', 250)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Register notification for initial copy completion.
    let parent_sync_done = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    parent_sync_done.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    let table_rows = destination.get_table_rows().await;
    let total_rows: usize = table_rows.values().map(|rows| rows.len()).sum();

    assert_eq!(total_rows, 3);

    let table_states = state_store.get_table_replication_states().await;

    assert!(table_states.contains_key(&parent_table_id));
    assert_eq!(table_states.len(), 1);

    let parent_table_rows = table_rows
        .iter()
        .filter(|(table_id, _)| **table_id == parent_table_id)
        .map(|(_, rows)| rows.len())
        .sum::<usize>();
    assert_eq!(parent_table_rows, 3);
}

/// Tests that CDC streams inserts to partitions created after pipeline startup.
/// New partitions are automatically included without publication changes.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_table_copy_and_streams_new_data_from_new_partition() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_late");
    let initial_partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _initial_partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &initial_partition_specs)
            .await
            .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_late".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Register notification for initial copy completion.
    let parent_sync_done = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    parent_sync_done.notified().await;

    let new_partition_name = format!("{}_{}", table_name.name, "p3");
    let new_partition_qualified_name = format!("{}.{}", table_name.schema, new_partition_name);
    database
        .run_sql(&format!(
            "create table {} partition of {} for values from (200) to (300)",
            new_partition_qualified_name,
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Wait for CDC to deliver the new row.
    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event3', 250)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    inserts_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    let table_rows = destination.get_table_rows().await;
    let total_rows: usize = table_rows.values().map(|rows| rows.len()).sum();
    assert_eq!(total_rows, 2);

    let table_states = state_store.get_table_replication_states().await;
    assert!(table_states.contains_key(&parent_table_id));
    assert_eq!(table_states.len(), 1);

    let parent_table_rows = table_rows
        .iter()
        .filter(|(table_id, _)| **table_id == parent_table_id)
        .map(|(_, rows)| rows.len())
        .sum::<usize>();
    assert_eq!(parent_table_rows, 2);

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let parent_inserts = grouped
        .get(&(EventType::Insert, parent_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(parent_inserts.len(), 1);
}

/// Tests that detaching and dropping a partition does not emit DELETE or TRUNCATE events.
/// Partition management is a DDL operation, not DML, so no data events should be generated.
#[tokio::test(flavor = "multi_thread")]
async fn partition_drop_does_not_emit_delete_or_truncate() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_drop");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_drop".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let parent_sync_done = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    parent_sync_done.notified().await;

    let events_before = destination.get_events().await;
    let grouped_before = group_events_by_type_and_table_id(&events_before);
    let delete_count_before = grouped_before
        .get(&(EventType::Delete, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);
    let truncate_count_before = grouped_before
        .get(&(EventType::Truncate, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);

    // Detach and drop one child partition (DDL should not generate DML events).
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified = format!("{}.{}", table_name.schema, partition_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            partition_p1_qualified
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!("drop table {partition_p1_qualified}"))
        .await
        .unwrap();

    let _ = pipeline.shutdown_and_wait().await;

    let events_after = destination.get_events().await;
    let grouped_after = group_events_by_type_and_table_id(&events_after);
    let delete_count_after = grouped_after
        .get(&(EventType::Delete, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);
    let truncate_count_after = grouped_after
        .get(&(EventType::Truncate, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);

    assert_eq!(delete_count_after, delete_count_before);
    assert_eq!(truncate_count_after, truncate_count_before);
}

/// Tests that issuing a TRUNCATE at the parent table level does emit a TRUNCATE event in the
/// replication stream.
#[tokio::test(flavor = "multi_thread")]
async fn parent_table_truncate_does_emit_truncate_event() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_truncate");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_truncate".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let parent_sync_done = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    parent_sync_done.notified().await;

    // Wait for the parent table truncate to be replicated.
    let truncate_notify = destination
        .wait_for_events_count(vec![(EventType::Truncate, 1)])
        .await;

    // We truncate the parent table.
    database
        .run_sql(&format!(
            "truncate table {}",
            table_name.as_quoted_identifier(),
        ))
        .await
        .unwrap();

    truncate_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let truncate_count = grouped_events
        .get(&(EventType::Truncate, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);

    assert_eq!(truncate_count, 1);
}

/// Tests that issuing a TRUNCATE at the child table level does NOT emit a TRUNCATE event in the
/// replication stream.
#[tokio::test(flavor = "multi_thread")]
async fn child_table_truncate_does_not_emit_truncate_event() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_truncate");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_truncate".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let parent_sync_done = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    parent_sync_done.notified().await;

    // We truncate the child table.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified = format!("{}.{}", table_name.schema, partition_p1_name);
    database
        .run_sql(&format!("truncate table {partition_p1_qualified}"))
        .await
        .unwrap();

    let _ = pipeline.shutdown_and_wait().await;

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let truncate_count = grouped_events
        .get(&(EventType::Truncate, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);

    assert_eq!(truncate_count, 0);
}

/// Tests that detached partitions are not replicated with explicit publications.
/// Once detached, the partition becomes independent and is not in the publication since
/// only the parent table was explicitly added. Inserts to detached partitions are not replicated.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_explicit_publication_does_not_replicate_detached_inserts() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_detach");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    let p1_table_id = partition_table_ids[0];

    // Insert initial data into both partitions.
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create explicit publication for parent table only.
    let publication_name = "test_partitioned_pub_detach".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let parent_sync_done = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    parent_sync_done.notified().await;

    // Verify initial sync copied both rows.
    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.len(), 1);
    let parent_rows: usize = table_rows
        .get(&parent_table_id)
        .map(|rows| rows.len())
        .unwrap_or(0);
    assert_eq!(parent_rows, 2);

    // Detach partition p1 from parent.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified = format!("{}.{}", table_name.schema, partition_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            partition_p1_qualified
        ))
        .await
        .unwrap();

    // Insert into the detached partition (should NOT be replicated).
    database
        .run_sql(&format!(
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', 25)"
        ))
        .await
        .unwrap();

    // Wait for the parent table insert to be replicated.
    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    // Insert into the parent table (should be replicated to remaining partition p2).
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('parent_event', 125)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    inserts_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    // Verify events
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    // Parent table should have 1 insert event (the insert after detachment).
    let parent_inserts = grouped
        .get(&(EventType::Insert, parent_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(parent_inserts.len(), 1);

    // Detached partition should have NO insert events.
    let detached_inserts = grouped
        .get(&(EventType::Insert, p1_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(detached_inserts.len(), 0);
}

/// Tests catalog state when a partition is detached with FOR ALL TABLES publication.
/// The detached partition appears in pg_publication_tables but is not automatically discovered
/// by the running pipeline. Table discovery only happens at pipeline startup, not during execution.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_all_tables_publication_does_not_replicate_detached_inserts() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_all_tables");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    let p1_table_id = partition_table_ids[0];

    // Insert initial data.
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create FOR ALL TABLES publication.
    let publication_name = "test_all_tables_pub_detach".to_string();
    database
        .create_publication_for_all(&publication_name, None)
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let parent_sync_done = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    parent_sync_done.notified().await;

    // Verify the initial state. The parent table is the only table tracked.
    let table_states_before = state_store.get_table_replication_states().await;
    assert!(table_states_before.contains_key(&parent_table_id));
    assert!(!table_states_before.contains_key(&p1_table_id));

    // Detach partition p1.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified = format!("{}.{}", table_name.schema, partition_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            partition_p1_qualified
        ))
        .await
        .unwrap();

    // Verify catalog state. The detached partition is now a standalone table.
    let inherits_check = database
        .client
        .as_ref()
        .unwrap()
        .query(
            "select count(*) as cnt from pg_inherits where inhrelid = $1",
            &[&p1_table_id.0],
        )
        .await
        .unwrap();
    let inherits_count: i64 = inherits_check[0].get("cnt");
    assert_eq!(inherits_count, 0);

    // Check pg_publication_tables. With FOR ALL TABLES, the detached partition should appear.
    let pub_tables_check = database
        .client
        .as_ref()
        .unwrap()
        .query(
            "select count(*) as cnt from pg_publication_tables
             where pubname = $1 and tablename = $2",
            &[&publication_name, &partition_p1_name],
        )
        .await
        .unwrap();
    let pub_tables_count: i64 = pub_tables_check[0].get("cnt");
    assert_eq!(pub_tables_count, 1);

    // Insert into detached partition.
    database
        .run_sql(&format!(
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', 25)"
        ))
        .await
        .unwrap();

    // Note: The running pipeline won't automatically discover the detached partition
    // without re-scanning for new tables. This is expected behavior, the table discovery
    // happens at pipeline start or explicit refresh.

    let _ = pipeline.shutdown_and_wait().await;

    // The pipeline state should still only track the parent table (not the detached partition)
    // because it hasn't re-scanned for new tables.
    let table_states_after = state_store.get_table_replication_states().await;
    assert!(table_states_after.contains_key(&parent_table_id));

    // The detached partition insert should NOT be replicated in this pipeline run
    // because the pipeline hasn't discovered it as a new table.
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let detached_inserts = grouped
        .get(&(EventType::Insert, p1_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(detached_inserts.len(), 0);
}

/// Tests that a detached partition is discovered as a new table after pipeline restart.
/// With FOR ALL TABLES publication, the detached partition is re-discovered during table
/// scanning at startup and its data is replicated.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_all_tables_publication_does_replicate_detached_inserts_on_restart() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_restart");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    let p1_table_id = partition_table_ids[0];

    // Insert initial data.
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create FOR ALL TABLES publication.
    let publication_name = "test_all_tables_restart".to_string();
    database
        .create_publication_for_all(&publication_name, None)
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Start pipeline and wait for initial sync.
    let parent_sync_done = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    parent_sync_done.notified().await;

    // Verify the initial state. The parent table is the only table tracked.
    let table_states_before = state_store.get_table_replication_states().await;
    assert!(table_states_before.contains_key(&parent_table_id));
    assert!(!table_states_before.contains_key(&p1_table_id));

    // Detach partition p1.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified = format!("{}.{}", table_name.schema, partition_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            partition_p1_qualified
        ))
        .await
        .unwrap();

    // Insert into detached partition (while pipeline is stopped).
    database
        .run_sql(&format!(
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', 25)"
        ))
        .await
        .unwrap();

    // Shutdown the pipeline.
    let _ = pipeline.shutdown_and_wait().await;

    // Restart the pipeline. It should now discover the detached partition as a new table.
    let detached_sync_done = state_store
        .notify_on_table_state_type(p1_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // Wait for the detached partition to be synced.
    detached_sync_done.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    // Verify the detached partition was discovered and synced.
    let table_states_after = state_store.get_table_replication_states().await;
    assert!(table_states_after.contains_key(&p1_table_id));

    // Verify the data from the detached partition was copied.
    let table_rows = destination.get_table_rows().await;
    let parent_rows: usize = table_rows
        .get(&parent_table_id)
        .map(|rows| rows.len())
        .unwrap_or(0);
    assert_eq!(parent_rows, 2);
    let detached_rows: usize = table_rows
        .get(&p1_table_id)
        .map(|rows| rows.len())
        .unwrap_or(0);
    assert_eq!(detached_rows, 2);
}

/// Tests that detached partitions are not automatically discovered with FOR TABLES IN SCHEMA publication.
/// Similar to FOR ALL TABLES, the detached partition appears in pg_publication_tables but is not
/// automatically discovered by the running pipeline without restart.
/// Requires PostgreSQL 15+ for FOR TABLES IN SCHEMA support.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_schema_publication_does_not_replicate_detached_inserts() {
    init_test_tracing();
    let database = spawn_source_database().await;

    // Skip test if PostgreSQL version is < 15 (FOR TABLES IN SCHEMA requires 15+).
    if let Some(version) = database.server_version() {
        if version.get() < 150000 {
            eprintln!("Skipping test: PostgreSQL 15+ required for FOR TABLES IN SCHEMA");
            return;
        }
    }

    let table_name = test_table_name("partitioned_events_schema_detach");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    let p1_table_id = partition_table_ids[0];

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create FOR TABLES IN SCHEMA publication.
    let publication_name = "test_schema_pub_detach".to_string();
    database
        .create_publication_for_all(&publication_name, Some(&table_name.schema))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let parent_sync_done = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    parent_sync_done.notified().await;

    // Verify initial state.
    let table_states_before = state_store.get_table_replication_states().await;
    assert!(table_states_before.contains_key(&parent_table_id));
    assert!(!table_states_before.contains_key(&p1_table_id));

    // Detach partition p1.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified = format!("{}.{}", table_name.schema, partition_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            partition_p1_qualified
        ))
        .await
        .unwrap();

    // Verify catalog state. The detached partition should appear in pg_publication_tables.
    let pub_tables_check = database
        .client
        .as_ref()
        .unwrap()
        .query(
            "select count(*) as cnt from pg_publication_tables
             where pubname = $1 and tablename = $2",
            &[&publication_name, &partition_p1_name],
        )
        .await
        .unwrap();
    let pub_tables_count: i64 = pub_tables_check[0].get("cnt");
    assert_eq!(pub_tables_count, 1);

    // Insert into detached partition.
    database
        .run_sql(&format!(
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', 25)"
        ))
        .await
        .unwrap();

    // Wait for the parent table insert to be replicated.
    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    // Insert into parent table (should be replicated).
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('parent_event', 125)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    inserts_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    // The pipeline state should still only track the parent table.
    let table_states_after = state_store.get_table_replication_states().await;
    assert!(table_states_after.contains_key(&parent_table_id));

    // Verify events.
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    // Parent table should have 1 insert event.
    let parent_inserts = grouped
        .get(&(EventType::Insert, parent_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(parent_inserts.len(), 1);

    // Detached partition inserts should NOT be replicated without table re-discovery.
    let detached_inserts = grouped
        .get(&(EventType::Insert, p1_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(detached_inserts.len(), 0);
}

/// Tests that a detached partition is discovered as a new table after pipeline restart
/// with FOR TABLES IN SCHEMA publication. After restart, the detached partition in the same
/// schema should be discovered and its data replicated.
/// Requires PostgreSQL 15+ for FOR TABLES IN SCHEMA support.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_schema_publication_does_replicate_detached_inserts_on_restart() {
    init_test_tracing();
    let database = spawn_source_database().await;

    // Skip test if PostgreSQL version is < 15 (FOR TABLES IN SCHEMA requires 15+).
    if let Some(version) = database.server_version() {
        if version.get() < 150000 {
            eprintln!("Skipping test: PostgreSQL 15+ required for FOR TABLES IN SCHEMA");
            return;
        }
    }

    let table_name = test_table_name("partitioned_events_schema_restart");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    let p1_table_id = partition_table_ids[0];

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create FOR TABLES IN SCHEMA publication.
    let publication_name = "test_schema_pub_restart".to_string();
    database
        .create_publication_for_all(&publication_name, Some(&table_name.schema))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Start pipeline and wait for initial sync.
    let parent_sync_done = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    parent_sync_done.notified().await;

    // Verify initial state.
    let table_states_before = state_store.get_table_replication_states().await;
    assert!(table_states_before.contains_key(&parent_table_id));
    assert!(!table_states_before.contains_key(&p1_table_id));

    // Detach partition p1.
    let partition_p1_name = format!("{}_{}", table_name.name, "p1");
    let partition_p1_qualified = format!("{}.{}", table_name.schema, partition_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            partition_p1_qualified
        ))
        .await
        .unwrap();

    // Insert into detached partition (while pipeline is still running).
    database
        .run_sql(&format!(
            "insert into {partition_p1_qualified} (data, partition_key) values ('detached_event', 25)"
        ))
        .await
        .unwrap();

    // Shutdown the pipeline.
    let _ = pipeline.shutdown_and_wait().await;

    // Restart the pipeline. It should now discover the detached partition as a new table.
    let detached_sync_done = state_store
        .notify_on_table_state_type(p1_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // Wait for the detached partition to be synced.
    detached_sync_done.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    // Verify the detached partition was discovered and synced.
    let table_states_after = state_store.get_table_replication_states().await;
    assert!(table_states_after.contains_key(&p1_table_id));

    // Verify the data from the detached partition was copied.
    let table_rows = destination.get_table_rows().await;
    let parent_rows: usize = table_rows
        .get(&parent_table_id)
        .map(|rows| rows.len())
        .unwrap_or(0);
    assert_eq!(parent_rows, 2);
    let detached_rows: usize = table_rows
        .get(&p1_table_id)
        .map(|rows| rows.len())
        .unwrap_or(0);
    assert_eq!(detached_rows, 2);
}

/// Tests that the system doesn't crash abruptly `publish_via_partition_root` is set to `false`.
///
/// The current behavior is to silently not perform replication, but we might want to refine this behavior
/// and throw an error when we detect that there are partitioned tables in a publication and the setting
/// is `false`. This way, we would be able to avoid forcing the user to always set `publish_via_partition_root=true`
/// when it's unnecessary.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_table_with_publish_via_root_false() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub".to_string();
    database
        .create_publication_with_config(&publication_name, std::slice::from_ref(&table_name), false)
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    // Wait on the sync done of the parent.
    let parent_sync_done = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    // Wait on the sync done of the parent.
    parent_sync_done.notified().await;

    // Wait for the COMMIT event of the insert in the parent table. COMMIT events are always
    // processed unconditionally because they don't contain relation-specific information.
    //
    // We use the COMMIT event to verify transaction processing: we can check whether the
    // transaction's component events were captured. In this case, they should NOT be present
    // because when `publication_via_partition_root` is `false`, events are tagged with child
    // table OIDs. Since these child table OIDs are unknown to us (we always try to find the parent oid),
    // those events are skipped.
    let commit = destination
        .wait_for_events_count(vec![(EventType::Commit, 1)])
        .await;

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    commit.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // No inserts should be captured for the reasons explained above.
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let parent_inserts = grouped_events
        .get(&(EventType::Insert, parent_table_id))
        .cloned()
        .unwrap_or_default();
    assert!(parent_inserts.is_empty());
}
