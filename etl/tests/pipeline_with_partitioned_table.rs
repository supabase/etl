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

/// The initial copy for a partitioned table (published via root) copies all existing rows.
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
            .expect("Failed to create partitioned table");

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values
             ('event1', 50), ('event2', 150), ('event3', 250)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

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

    assert_eq!(
        total_rows, 3,
        "Expected 3 rows synced (one per partition), but got {total_rows}"
    );

    let table_states = state_store.get_table_replication_states().await;

    assert!(
        table_states.contains_key(&parent_table_id),
        "Parent table should be tracked in state"
    );
    assert_eq!(
        table_states.len(),
        1,
        "Only the parent table should be tracked in state"
    );

    let parent_table_rows = table_rows
        .iter()
        .filter(|(table_id, _)| **table_id == parent_table_id)
        .map(|(_, rows)| rows.len())
        .sum::<usize>();
    assert_eq!(
        parent_table_rows, 3,
        "Parent table should contain all rows when publishing via root"
    );
}

/// The initial copy completes and CDC streams new rows from newly added partitions.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_table_copy_and_streams_new_data_from_new_partition() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_late");
    let initial_partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _initial_partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &initial_partition_specs)
            .await
            .expect("Failed to create initial partitioned table");

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values \
             ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_late".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

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

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event3', 250)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Wait for CDC to deliver the new row.
    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;
    inserts_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    let table_rows = destination.get_table_rows().await;
    let total_rows: usize = table_rows.values().map(|rows| rows.len()).sum();
    assert_eq!(
        total_rows, 2,
        "Expected 2 rows synced from initial copy, got {total_rows}"
    );

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

/// Dropping a child partition must not emit DELETE/TRUNCATE events.
#[tokio::test(flavor = "multi_thread")]
async fn partition_drop_does_not_emit_delete_or_truncate() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_drop");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .expect("Failed to create partitioned table");

    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values \
             ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_pub_drop".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

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
    let del_before = grouped_before
        .get(&(EventType::Delete, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);
    let trunc_before = grouped_before
        .get(&(EventType::Truncate, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);

    // Detach and drop one child partition (DDL should not generate DML events)
    let child_p1_name = format!("{}_{}", table_name.name, "p1");
    let child_p1_qualified = format!("{}.{}", table_name.schema, child_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            child_p1_qualified
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!("drop table {child_p1_qualified}"))
        .await
        .unwrap();

    let _ = pipeline.shutdown_and_wait().await;

    let events_after = destination.get_events().await;
    let grouped_after = group_events_by_type_and_table_id(&events_after);
    let del_after = grouped_after
        .get(&(EventType::Delete, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);
    let trunc_after = grouped_after
        .get(&(EventType::Truncate, parent_table_id))
        .map(|v| v.len())
        .unwrap_or(0);

    assert_eq!(
        del_after, del_before,
        "Partition drop must not emit DELETE events"
    );
    assert_eq!(
        trunc_after, trunc_before,
        "Partition drop must not emit TRUNCATE events"
    );
}

/// When a partition is detached from a partitioned table with an explicit publication,
/// inserts into the detached partition should NOT be replicated since only the parent
/// table is in the publication.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_explicit_publication_does_not_replicate_detached_inserts() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_detach");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .expect("Failed to create partitioned table");

    let p1_table_id = partition_table_ids[0];

    // Insert initial data into both partitions
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values \
             ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create explicit publication for parent table only
    let publication_name = "test_partitioned_pub_detach".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

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

    // Verify initial sync copied both rows
    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.len(), 1);
    let parent_rows: usize = table_rows
        .get(&parent_table_id)
        .map(|rows| rows.len())
        .unwrap_or(0);
    assert_eq!(
        parent_rows, 2,
        "Parent table should have 2 rows from initial COPY"
    );

    // Detach partition p1 from parent
    let child_p1_name = format!("{}_{}", table_name.name, "p1");
    let child_p1_qualified = format!("{}.{}", table_name.schema, child_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            child_p1_qualified
        ))
        .await
        .unwrap();

    // Insert into the detached partition (should NOT be replicated)
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('detached_event', 25)",
            child_p1_qualified
        ))
        .await
        .unwrap();

    // Insert into parent table (should be replicated to remaining partition p2)
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('parent_event', 125)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Wait for the parent table insert to be replicated
    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;
    inserts_notify.notified().await;

    let _ = pipeline.shutdown_and_wait().await;

    // Verify events
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    // Parent table should have 1 insert event (the insert after detachment)
    let parent_inserts = grouped
        .get(&(EventType::Insert, parent_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        parent_inserts.len(),
        1,
        "Parent table should have exactly 1 CDC insert event"
    );

    // Detached partition should have NO insert events
    let detached_inserts = grouped
        .get(&(EventType::Insert, p1_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        detached_inserts.len(),
        0,
        "Detached partition inserts should NOT be replicated"
    );
}

// TODO: validate.
/// When a partition is detached from a partitioned table with FOR ALL TABLES publication,
/// the detached partition becomes a standalone table. However, the running pipeline won't
/// automatically discover it without re-scanning. This test validates the catalog state.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_all_tables_publication_catalog_state() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_all_tables");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .expect("Failed to create partitioned table");

    let p1_table_id = partition_table_ids[0];

    // Insert initial data
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values \
             ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create FOR ALL TABLES publication
    let publication_name = "test_all_tables_pub_detach".to_string();
    database
        .run_sql(&format!(
            "create publication {} for all tables with (publish_via_partition_root = true)",
            publication_name
        ))
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

    // Verify initial state: only parent table is tracked
    let table_states_before = state_store.get_table_replication_states().await;
    assert!(
        table_states_before.contains_key(&parent_table_id),
        "Parent table should be tracked before detachment"
    );
    assert!(
        !table_states_before.contains_key(&p1_table_id),
        "Child partition p1 should NOT be tracked separately before detachment"
    );

    // Detach partition p1
    let child_p1_name = format!("{}_{}", table_name.name, "p1");
    let child_p1_qualified = format!("{}.{}", table_name.schema, child_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            child_p1_qualified
        ))
        .await
        .unwrap();

    // Verify catalog state: detached partition is now a standalone table
    // Check pg_inherits - should no longer have parent relationship
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
    assert_eq!(
        inherits_count, 0,
        "Detached partition should have no parent in pg_inherits"
    );

    // Check pg_publication_tables - with FOR ALL TABLES, detached partition should appear
    let pub_tables_check = database
        .client
        .as_ref()
        .unwrap()
        .query(
            "select count(*) as cnt from pg_publication_tables
             where pubname = $1 and tablename = $2",
            &[&publication_name, &child_p1_name],
        )
        .await
        .unwrap();
    let pub_tables_count: i64 = pub_tables_check[0].get("cnt");
    assert_eq!(
        pub_tables_count, 1,
        "Detached partition should appear in pg_publication_tables for ALL TABLES publication"
    );

    // Insert into detached partition
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('detached_event', 25)",
            child_p1_qualified
        ))
        .await
        .unwrap();

    // Note: The running pipeline won't automatically discover the detached partition
    // without re-scanning for new tables. This is expected behavior - table discovery
    // happens at pipeline start or explicit refresh.

    let _ = pipeline.shutdown_and_wait().await;

    // The pipeline state should still only track the parent table (not the detached partition)
    // because it hasn't re-scanned for new tables
    let table_states_after = state_store.get_table_replication_states().await;
    assert!(
        table_states_after.contains_key(&parent_table_id),
        "Parent table should still be tracked after detachment"
    );

    // The detached partition insert should NOT be replicated in this pipeline run
    // because the pipeline hasn't discovered it as a new table
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let detached_inserts = grouped
        .get(&(EventType::Insert, p1_table_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        detached_inserts.len(),
        0,
        "Detached partition inserts should NOT be replicated without table re-discovery"
    );
}


// TODO: validate.
/// When a partition is detached and then the pipeline restarts (simulating table re-discovery),
/// the detached partition should be discovered as a new standalone table if using FOR ALL TABLES.
#[tokio::test(flavor = "multi_thread")]
async fn partition_detach_with_all_tables_and_pipeline_restart_discovers_new_table() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_events_restart");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs)
            .await
            .expect("Failed to create partitioned table");

    let p1_table_id = partition_table_ids[0];

    // Insert initial data
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values \
             ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create FOR ALL TABLES publication
    let publication_name = "test_all_tables_restart".to_string();
    database
        .run_sql(&format!(
            "create publication {} for all tables with (publish_via_partition_root = true)",
            publication_name
        ))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Start pipeline and wait for initial sync
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

    // Shutdown the first pipeline
    let _ = pipeline.shutdown_and_wait().await;

    // Detach partition p1
    let child_p1_name = format!("{}_{}", table_name.name, "p1");
    let child_p1_qualified = format!("{}.{}", table_name.schema, child_p1_name);
    database
        .run_sql(&format!(
            "alter table {} detach partition {}",
            table_name.as_quoted_identifier(),
            child_p1_qualified
        ))
        .await
        .unwrap();

    // Insert into detached partition (while pipeline is stopped)
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('detached_event', 25)",
            child_p1_qualified
        ))
        .await
        .unwrap();

    // Restart the pipeline - it should now discover the detached partition as a new table
    let state_store2 = NotifyingStore::new();
    let destination2 = TestDestinationWrapper::wrap(MemoryDestination::new());

    let detached_sync_done = state_store2
        .notify_on_table_state_type(p1_table_id, TableReplicationPhaseType::SyncDone)
        .await;

    let pipeline_id2: PipelineId = random();
    let mut pipeline2 = create_pipeline(
        &database.config,
        pipeline_id2,
        publication_name.clone(),
        state_store2.clone(),
        destination2.clone(),
    );

    pipeline2.start().await.unwrap();

    // Wait for detached partition to be synced
    detached_sync_done.notified().await;

    let _ = pipeline2.shutdown_and_wait().await;

    // Verify the detached partition was discovered and synced
    let table_states = state_store2.get_table_replication_states().await;
    assert!(
        table_states.contains_key(&p1_table_id),
        "Detached partition should be discovered as a standalone table after restart"
    );

    // Verify the data from the detached partition was copied
    let table_rows = destination2.get_table_rows().await;
    let detached_rows: usize = table_rows
        .get(&p1_table_id)
        .map(|rows| rows.len())
        .unwrap_or(0);
    assert!(
        detached_rows > 0,
        "Detached partition should have rows synced after pipeline restart"
    );
}
