use etl::{
    event::{Event, EventType},
    pipeline::PipelineId,
    schema::{SnapshotId, TableId, TableName},
    store::{SchemaStore, StateStore, TableState, TableStateType},
    test_utils::{
        database::{spawn_source_database, test_table_name},
        event::group_events_by_type_and_table_id,
        faults::FaultyOp,
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::{PipelineBuilder, create_pipeline, create_pipeline_with_batch_config},
        test_destination_wrapper::TestDestinationWrapper,
    },
};
use etl_config::shared::{BatchConfig, PublicationChangesMode};
use etl_postgres::{
    below_version, slots::EtlReplicationSlot, tokio::test_utils::PgDatabase, version::POSTGRES_15,
};
use etl_telemetry::tracing::init_test_tracing;
use pg_escape::quote_identifier;
use rand::random;
use tokio::time::sleep;
use tokio_postgres::Client;

/// Alters one publication after registering every expected lifecycle
/// notification.
async fn alter_publication_and_wait_for_table_states(
    database: &PgDatabase<Client>,
    store: &NotifyingStore,
    publication_name: &str,
    alteration: &str,
    visibility_table: &TableName,
    ready_table_ids: &[TableId],
    removed_table_ids: &[TableId],
) {
    let mut ready = Vec::with_capacity(ready_table_ids.len());
    for table_id in ready_table_ids {
        ready.push(store.notify_on_table_state_type(*table_id, TableStateType::Ready).await);
    }

    let mut removed = Vec::with_capacity(removed_table_ids.len());
    for table_id in removed_table_ids {
        removed.push(store.notify_on_table_state_removed(*table_id).await);
    }

    database
        .client
        .as_ref()
        .unwrap()
        .batch_execute(&format!(
            "begin;
             alter publication {} {alteration};
             insert into {} (value) values (0);
             commit;",
            quote_identifier(publication_name),
            visibility_table.as_quoted_identifier()
        ))
        .await
        .unwrap();

    for notification in ready {
        notification.notified().await;
    }
    for notification in removed {
        notification.notified().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_publication_additions_are_reconciled_on_restart() {
    init_test_tracing();

    let database = spawn_source_database().await;

    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for FOR TABLES IN SCHEMA");
        return;
    }

    // Create two tables in the test schema and a publication for that schema.
    let table_1 = test_table_name("table_1");
    let table_1_id =
        database.create_table(table_1.clone(), true, &[("value", "int4 not null")]).await.unwrap();
    let table_2 = test_table_name("table_2");
    let table_2_id =
        database.create_table(table_2.clone(), true, &[("value", "int4 not null")]).await.unwrap();

    let publication_name = "test_pub_cleanup";
    database.create_publication_for_all(publication_name, Some(&table_1.schema)).await.unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    )
    .build();

    // Wait for initial copy completion (Ready) for both tables.
    let table_1_ready_notify =
        store.notify_on_table_state_type(table_1_id, TableStateType::Ready).await;
    let table_2_ready_notify =
        store.notify_on_table_state_type(table_2_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    table_1_ready_notify.notified().await;
    table_2_ready_notify.notified().await;

    // Insert one row in each table and wait for two insert events.
    let inserts_notify = destination.wait_for_events_count(vec![(EventType::Insert, 2)]).await;

    database.insert_values(table_1.clone(), &["value"], &[&1]).await.unwrap();
    database.insert_values(table_2.clone(), &["value"], &[&1]).await.unwrap();

    inserts_notify.notified().await;

    // Drop table_2 so it's no longer part of the publication.
    database
        .client
        .as_ref()
        .unwrap()
        .execute(&format!("drop table {}", table_2.as_quoted_identifier()), &[])
        .await
        .unwrap();

    // Reactive handling owns the removal from WAL.
    pipeline.shutdown_and_wait().await.unwrap();

    // The destination should have the insert event for each original table
    // before the restart.
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let table_1_inserts = grouped.get(&(EventType::Insert, table_1_id)).cloned().unwrap();
    assert_eq!(table_1_inserts.len(), 1);
    let table_2_inserts = grouped.get(&(EventType::Insert, table_2_id)).cloned().unwrap();
    assert_eq!(table_2_inserts.len(), 1);

    destination.clear_events().await;

    // Create table_3 which is going to be added to the publication.
    let table_3 = test_table_name("table_3");
    let table_3_id =
        database.create_table(table_3.clone(), true, &[("value", "int4 not null")]).await.unwrap();

    // Restart pipeline; it should detect table_2 is gone and purge its state
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    )
    .build();

    // Wait for the table_3 to be done.
    let table_3_ready_notify =
        store.notify_on_table_state_type(table_3_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    table_3_ready_notify.notified().await;

    // Insert one row in table_1 and table_3 and wait for the new events.
    let inserts_notify = destination.wait_for_events_count(vec![(EventType::Insert, 2)]).await;

    database.insert_values(table_1.clone(), &["value"], &[&2]).await.unwrap();
    database.insert_values(table_3.clone(), &["value"], &[&1]).await.unwrap();

    inserts_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Reactive startup is additive. The dropped table remains until a WAL
    // publication-change message explicitly removes it, while the new schema
    // member is discovered from the current publication snapshot.
    let states = store.get_table_states().await;
    assert!(states.contains_key(&table_1_id));
    assert!(states.contains_key(&table_2_id));
    assert!(states.contains_key(&table_3_id));

    // Assert that the table sync slot for table_2 is also deleted.
    let table_2_slot_name: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_2_id).try_into().unwrap();
    let slot_state = database.get_replication_slot_state(&table_2_slot_name).await.unwrap();
    assert_eq!(slot_state, None, "Table sync slot for removed table should be deleted");

    // The destination should have the new event for table_1 and table_3.
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let table_1_inserts = grouped.get(&(EventType::Insert, table_1_id)).cloned().unwrap();
    assert_eq!(table_1_inserts.len(), 1);
    let table_3_inserts = grouped.get(&(EventType::Insert, table_3_id)).cloned().unwrap();
    assert_eq!(table_3_inserts.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_alter_membership_forms_are_applied_at_runtime() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_1 = test_table_name("publication_matrix_table_1");
    let table_1_id =
        database.create_table(table_1.clone(), true, &[("value", "int4 not null")]).await.unwrap();
    let table_2 = test_table_name("publication_matrix_table_2");
    let table_2_id =
        database.create_table(table_2.clone(), true, &[("value", "int4 not null")]).await.unwrap();
    let table_3 = test_table_name("publication_matrix_table_3");
    let table_3_id =
        database.create_table(table_3.clone(), true, &[("value", "int4 not null")]).await.unwrap();
    let visibility_table = test_table_name("publication_matrix_visibility");
    let visibility_table_id = database
        .create_table(visibility_table.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();
    let partition_root = test_table_name("publication_matrix_partition_root");
    let partition_leaf = test_table_name("publication_matrix_partition_leaf");
    database
        .client
        .as_ref()
        .unwrap()
        .batch_execute(&format!(
            "create table {} (
                 id bigint primary key,
                 value int4 not null
             ) partition by range (id);
             create table {} partition of {} for values from (0) to (100);",
            partition_root.as_quoted_identifier(),
            partition_leaf.as_quoted_identifier(),
            partition_root.as_quoted_identifier()
        ))
        .await
        .unwrap();
    let partition_root_id = database
        .client
        .as_ref()
        .unwrap()
        .query_one(
            "select c.oid
             from pg_catalog.pg_class c
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace
             where n.nspname = $1 and c.relname = $2",
            &[&partition_root.schema, &partition_root.name],
        )
        .await
        .unwrap()
        .get(0);
    let partition_leaf_id = database
        .client
        .as_ref()
        .unwrap()
        .query_one(
            "select c.oid
             from pg_catalog.pg_class c
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace
             where n.nspname = $1 and c.relname = $2",
            &[&partition_leaf.schema, &partition_leaf.name],
        )
        .await
        .unwrap()
        .get(0);

    let publication_name = "test_publication_alter_matrix";
    database
        .create_publication(publication_name, std::slice::from_ref(&visibility_table))
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let mut pipeline = create_pipeline(
        &database.config,
        random(),
        publication_name.to_owned(),
        store.clone(),
        destination,
    );
    let visibility_ready =
        store.notify_on_table_state_type(visibility_table_id, TableStateType::Ready).await;
    pipeline.start().await.unwrap();
    visibility_ready.notified().await;

    alter_publication_and_wait_for_table_states(
        &database,
        &store,
        publication_name,
        &format!(
            "add table {}, {}",
            table_1.as_quoted_identifier(),
            table_2.as_quoted_identifier()
        ),
        &visibility_table,
        &[table_1_id, table_2_id],
        &[],
    )
    .await;
    alter_publication_and_wait_for_table_states(
        &database,
        &store,
        publication_name,
        &format!("drop table only {}", table_1.as_quoted_identifier()),
        &visibility_table,
        &[],
        &[table_1_id],
    )
    .await;
    alter_publication_and_wait_for_table_states(
        &database,
        &store,
        publication_name,
        &format!(
            "set table {}, {}",
            visibility_table.as_quoted_identifier(),
            table_3.as_quoted_identifier()
        ),
        &visibility_table,
        &[table_3_id],
        &[table_2_id],
    )
    .await;

    alter_publication_and_wait_for_table_states(
        &database,
        &store,
        publication_name,
        &format!("add table {}", partition_root.as_quoted_identifier()),
        &visibility_table,
        &[partition_root_id],
        &[],
    )
    .await;
    alter_publication_and_wait_for_table_states(
        &database,
        &store,
        publication_name,
        "set (publish_via_partition_root = false)",
        &visibility_table,
        &[partition_leaf_id],
        &[partition_root_id],
    )
    .await;
    alter_publication_and_wait_for_table_states(
        &database,
        &store,
        publication_name,
        "set (publish_via_partition_root = true)",
        &visibility_table,
        &[partition_root_id],
        &[partition_leaf_id],
    )
    .await;

    let mut expected_table_ids = vec![visibility_table_id, table_3_id, partition_root_id];
    if !below_version!(database.server_version(), POSTGRES_15) {
        let schema_a = "publication_matrix_schema_a";
        let schema_b = "publication_matrix_schema_b";
        database
            .client
            .as_ref()
            .unwrap()
            .batch_execute(&format!(
                "create schema {}; create schema {}",
                quote_identifier(schema_a),
                quote_identifier(schema_b)
            ))
            .await
            .unwrap();
        let schema_table_a = TableName::new(schema_a.to_owned(), "table_a".to_owned());
        let schema_table_a_id = database
            .create_table(schema_table_a, true, &[("value", "int4 not null")])
            .await
            .unwrap();
        let schema_table_b = TableName::new(schema_b.to_owned(), "table_b".to_owned());
        let schema_table_b_id = database
            .create_table(schema_table_b, true, &[("value", "int4 not null")])
            .await
            .unwrap();

        alter_publication_and_wait_for_table_states(
            &database,
            &store,
            publication_name,
            &format!("add tables in schema {}", quote_identifier(schema_a)),
            &visibility_table,
            &[schema_table_a_id],
            &[],
        )
        .await;
        alter_publication_and_wait_for_table_states(
            &database,
            &store,
            publication_name,
            &format!(
                "add table {}, tables in schema {}",
                table_1.as_quoted_identifier(),
                quote_identifier(schema_b)
            ),
            &visibility_table,
            &[table_1_id, schema_table_b_id],
            &[],
        )
        .await;
        alter_publication_and_wait_for_table_states(
            &database,
            &store,
            publication_name,
            &format!("drop tables in schema {}", quote_identifier(schema_a)),
            &visibility_table,
            &[],
            &[schema_table_a_id],
        )
        .await;
        alter_publication_and_wait_for_table_states(
            &database,
            &store,
            publication_name,
            &format!(
                "set table {}, {}, {}, tables in schema {}",
                visibility_table.as_quoted_identifier(),
                table_2.as_quoted_identifier(),
                partition_root.as_quoted_identifier(),
                quote_identifier(schema_a)
            ),
            &visibility_table,
            &[table_2_id, schema_table_a_id],
            &[table_1_id, table_3_id, schema_table_b_id],
        )
        .await;

        expected_table_ids =
            vec![visibility_table_id, table_2_id, partition_root_id, schema_table_a_id];
    }

    pipeline.shutdown_and_wait().await.unwrap();

    let states = store.get_table_states().await;
    assert_eq!(states.len(), expected_table_ids.len());
    for table_id in expected_table_ids {
        assert_eq!(states.get(&table_id).map(TableState::as_type), Some(TableStateType::Ready));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_changes_are_applied_at_runtime() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let table = test_table_name("runtime_publication_table");
    let table_id =
        database.create_table(table.clone(), true, &[("value", "int4 not null")]).await.unwrap();
    database.insert_values(table.clone(), &["value"], &[&1]).await.unwrap();

    let publication_name = "test_runtime_publication_changes";
    database.create_publication(publication_name, std::slice::from_ref(&table)).await.unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let mut pipeline = create_pipeline_with_batch_config(
        &database.config,
        random(),
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
        BatchConfig { max_fill_ms: 100, memory_budget_ratio: 0.2, max_bytes: 8 * 1024 * 1024 },
    );

    let table_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    pipeline.start().await.unwrap();
    table_ready.notified().await;

    let held_write = destination.hold_next(FaultyOp::WriteEvents).await;
    let table_removed = store.notify_on_table_state_removed(table_id).await;
    let pending_insert = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;
    let transaction = database.client.as_mut().unwrap().transaction().await.unwrap();
    transaction
        .execute(&format!("insert into {} (value) values (2)", table.as_quoted_identifier()), &[])
        .await
        .unwrap();
    transaction
        .execute(
            &format!(
                "alter publication {} drop table {}",
                quote_identifier(publication_name),
                table.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    held_write.wait_reached().await;
    assert!(store.get_table_state(table_id).await.unwrap().is_some());
    assert!(store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap().is_some());
    assert!(store.get_applied_destination_table_metadata(table_id).await.unwrap().is_some());

    let table_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    let transaction = database.client.as_mut().unwrap().transaction().await.unwrap();
    transaction
        .execute(
            &format!(
                "alter publication {} add table {}",
                quote_identifier(publication_name),
                table.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();
    transaction
        .execute(&format!("insert into {} (value) values (3)", table.as_quoted_identifier()), &[])
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    held_write.release_ok();

    table_removed.notified().await;
    pending_insert.notified().await;
    table_ready.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    assert!(!destination.was_table_dropped_for_copy(table_id).await);
    assert!(store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap().is_some());
    let copied_rows = destination.get_table_rows().await;
    // Publication removal is not a reset. The destination object is retained,
    // so the wrapper records the original copy plus the later new-member copy.
    // The in-flight second row is recorded separately in streaming history.
    assert_eq!(copied_rows.get(&table_id).unwrap().len(), 4);
    assert_eq!(
        destination
            .get_events()
            .await
            .iter()
            .filter(|event| matches!(event, Event::Insert(insert) if insert.replicated_table_schema.id() == table_id))
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_changes_for_another_publication_are_ignored() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let replicated_table = test_table_name("configured_publication_table");
    let replicated_table_id = database
        .create_table(replicated_table.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();
    let unrelated_table = test_table_name("unrelated_publication_table");
    let unrelated_table_id = database
        .create_table(unrelated_table.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();
    let added_unrelated_table = test_table_name("added_unrelated_publication_table");
    let added_unrelated_table_id = database
        .create_table(added_unrelated_table.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();

    let publication_name = "test_configured_publication_changes";
    let unrelated_publication_name = "test_unrelated_publication_changes";
    database
        .create_publication(publication_name, std::slice::from_ref(&replicated_table))
        .await
        .unwrap();
    database
        .create_publication(unrelated_publication_name, std::slice::from_ref(&unrelated_table))
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let mut pipeline = create_pipeline(
        &database.config,
        random(),
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    );

    let ready = store.notify_on_table_state_type(replicated_table_id, TableStateType::Ready).await;
    pipeline.start().await.unwrap();
    ready.notified().await;

    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "alter publication {} add table {}",
                quote_identifier(unrelated_publication_name),
                added_unrelated_table.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();

    let insert_received = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;
    database.insert_values(replicated_table, &["value"], &[&1]).await.unwrap();
    insert_received.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let states = store.get_table_states().await;
    assert_eq!(states.len(), 1);
    assert!(states.contains_key(&replicated_table_id));
    assert!(!states.contains_key(&unrelated_table_id));
    assert!(!states.contains_key(&added_unrelated_table_id));
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_remove_and_readd_starts_new_copy_without_resetting_destination() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let table = test_table_name("transactional_publication_readd");
    let table_id =
        database.create_table(table.clone(), true, &[("value", "int4 not null")]).await.unwrap();
    database.insert_values(table.clone(), &["value"], &[&1]).await.unwrap();

    let publication_name = "test_transactional_publication_readd";
    database.create_publication(publication_name, std::slice::from_ref(&table)).await.unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let mut pipeline = create_pipeline_with_batch_config(
        &database.config,
        random(),
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
        BatchConfig { max_fill_ms: 100, memory_budget_ratio: 0.2, max_bytes: 8 * 1024 * 1024 },
    );

    let initial_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    pipeline.start().await.unwrap();
    initial_ready.notified().await;

    let table_removed = store.notify_on_table_state_removed(table_id).await;
    let replacement_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    let transaction = database.client.as_mut().unwrap().transaction().await.unwrap();
    transaction
        .execute(
            &format!(
                "alter publication {} drop table {}",
                quote_identifier(publication_name),
                table.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();
    transaction
        .execute(
            &format!(
                "alter publication {} add table {}",
                quote_identifier(publication_name),
                table.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();
    transaction
        .execute(&format!("insert into {} (value) values (2)", table.as_quoted_identifier()), &[])
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    table_removed.notified().await;
    replacement_ready.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    assert_eq!(
        store.get_table_state(table_id).await.unwrap().as_ref().map(TableState::as_type),
        Some(TableStateType::Ready)
    );
    // Removal and re-addition is intentionally not a table reset.
    assert!(!destination.was_table_dropped_for_copy(table_id).await);
    assert_eq!(destination.get_table_rows().await.get(&table_id).unwrap().len(), 3);
    assert!(
        destination
            .get_events()
            .await
            .iter()
            .all(|event| !matches!(event, Event::Insert(insert) if insert.replicated_table_schema.id() == table_id))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn disabled_publication_changes_are_ignored_across_restarts() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let existing_table = test_table_name("disabled_runtime_publication_existing");
    let existing_table_id = database
        .create_table(existing_table.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();
    let added_table = test_table_name("disabled_runtime_publication_added");
    let added_table_id = database
        .create_table(added_table.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();

    let publication_name = "test_disabled_runtime_publication_changes";
    database
        .create_publication(publication_name, std::slice::from_ref(&existing_table))
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let pipeline_id = random();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    )
    .with_publication_changes_mode(PublicationChangesMode::Disabled)
    .build();

    let existing_table_ready =
        store.notify_on_table_state_type(existing_table_id, TableStateType::Ready).await;
    pipeline.start().await.unwrap();
    existing_table_ready.notified().await;

    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "alter publication {} add table {}",
                quote_identifier(publication_name),
                added_table.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();

    let insert_received = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;
    database.insert_values(existing_table, &["value"], &[&1]).await.unwrap();
    insert_received.notified().await;

    assert!(store.get_table_state(added_table_id).await.unwrap().is_none());
    pipeline.shutdown_and_wait().await.unwrap();

    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination,
    )
    .with_publication_changes_mode(PublicationChangesMode::Disabled)
    .build();
    pipeline.start().await.unwrap();
    pipeline.shutdown_and_wait().await.unwrap();

    assert!(store.get_table_state(added_table_id).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_removal_aborts_active_table_sync_worker() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table = test_table_name("runtime_publication_active_sync");
    let table_id =
        database.create_table(table.clone(), true, &[("value", "int4 not null")]).await.unwrap();
    database.insert_values(table.clone(), &["value"], &[&1]).await.unwrap();
    let publication_name = "test_runtime_publication_active_sync";
    database.create_publication(publication_name, std::slice::from_ref(&table)).await.unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let pipeline_id = random();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    )
    .with_batch_config(BatchConfig {
        max_fill_ms: 100,
        memory_budget_ratio: 0.2,
        max_bytes: 8 * 1024 * 1024,
    })
    .build();

    let held_copy = destination.hold_next(FaultyOp::WriteTableRows).await;
    pipeline.start().await.unwrap();
    held_copy.wait_reached().await;

    let table_removed = store.notify_on_table_state_removed(table_id).await;
    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "alter publication {} drop table {}",
                quote_identifier(publication_name),
                table.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();
    table_removed.notified().await;
    assert!(store.get_table_state(table_id).await.unwrap().is_none());

    pipeline.shutdown_and_wait().await.unwrap();

    let slot_name: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_id).try_into().unwrap();
    assert_eq!(database.get_replication_slot_state(&slot_name).await.unwrap(), None);
}
#[tokio::test(flavor = "multi_thread")]
async fn publication_for_all_tables_in_schema_ignores_new_tables_until_restart() {
    init_test_tracing();

    let database = spawn_source_database().await;

    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for FOR TABLES IN SCHEMA");
        return;
    }

    // Create first table and insert one row.
    let table_1 = test_table_name("table_1");
    let table_1_id =
        database.create_table(table_1.clone(), true, &[("name", "text not null")]).await.unwrap();
    database.insert_values(table_1.clone(), &["name"], &[&"test_name_1".to_owned()]).await.unwrap();

    // Create a publication for all tables in the test schema.
    let publication_name = "test_pub_all_schema";
    database.create_publication_for_all(publication_name, Some(&table_1.schema)).await.unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    );

    let table_ready_notify =
        store.notify_on_table_state_type(table_1_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    table_ready_notify.notified().await;

    // Wait for an insert event in table 1.
    let insert_events_notify =
        destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

    database.insert_values(table_1.clone(), &["name"], &[&"test_name_2".to_owned()]).await.unwrap();

    insert_events_notify.notified().await;

    // Create a new table in the same schema and insert a row.
    let table_2 = test_table_name("table_2");
    let table_2_id =
        database.create_table(table_2.clone(), true, &[("value", "int4 not null")]).await.unwrap();
    database.insert_values(table_2.clone(), &["value"], &[&1_i32]).await.unwrap();

    // Wait for the events to come in from the new table to make sure the pipeline
    // reacts to them gracefully even if they are not replicated.
    sleep(Duration::from_secs(2)).await;

    // Shutdown and verify no errors occurred.
    pipeline.shutdown_and_wait().await.unwrap();

    // Check that only the schemas of the first table were stored.
    let table_schemas = store.get_latest_table_schemas().await;
    assert_eq!(table_schemas.len(), 1);
    assert!(table_schemas.contains_key(&table_1_id));
    assert!(!table_schemas.contains_key(&table_2_id));

    // Verify the table rows and events inserted into table 1.
    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.get(&table_1_id).unwrap().len(), 1);
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let insert_events = grouped_events.get(&(EventType::Insert, table_1_id)).unwrap();
    assert_eq!(insert_events.len(), 1);

    // We restart the pipeline and verify that the new table is now processed.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    );

    let table_ready_notify =
        store.notify_on_table_state_type(table_2_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    table_ready_notify.notified().await;

    // We clear the events to make waiting more idiomatic down the line.
    destination.clear_events().await;

    // Wait for an insert event in table 2.
    let insert_events_notify =
        destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

    database.insert_values(table_2.clone(), &["value"], &[&2_i32]).await.unwrap();

    insert_events_notify.notified().await;

    // Shutdown and verify no errors occurred.
    pipeline.shutdown_and_wait().await.unwrap();

    // Check that both schemas exist.
    let table_schemas = store.get_latest_table_schemas().await;
    assert_eq!(table_schemas.len(), 2);
    assert!(table_schemas.contains_key(&table_1_id));
    assert!(table_schemas.contains_key(&table_2_id));

    // Verify the table rows and events inserted into table 2.
    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.get(&table_2_id).unwrap().len(), 1);
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let insert_events = grouped_events.get(&(EventType::Insert, table_2_id)).unwrap();
    assert_eq!(insert_events.len(), 1);
}
use std::time::Duration;
