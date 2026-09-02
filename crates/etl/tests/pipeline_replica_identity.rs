use etl::{
    data::{Cell, OldTableRow, PartialTableRow, TableRow, UpdatedTableRow},
    event::{DeleteEvent, Event, EventType, UpdateEvent},
    pipeline::PipelineId,
    test_utils::{
        database::{spawn_source_database, test_table_name},
        event::EventCondition,
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
    },
};
use etl_postgres::tokio::test_utils::TableModification;
use etl_telemetry::tracing::init_test_tracing;
use pg_escape::{quote_identifier, quote_literal};
use rand::{Rng, distr::Alphanumeric, random};

const LARGE_TEXT_SIZE_BYTES: usize = 8192;
const INITIAL_ID: i64 = 1;
const INITIAL_NAME: &str = "alice";
const INITIAL_SURNAME: &str = "smith";
const INITIAL_CITY: &str = "rome";
const UPDATED_CITY: &str = "vienna";
const UPDATED_NAME_IDENTITY: &str = "alicia";
const UPDATED_SURNAME_IDENTITY: &str = "smithers";

#[derive(Clone, Copy)]
enum ReplicaIdentityMode {
    Default,
    Full,
    UsingIndex,
    Nothing,
}

impl ReplicaIdentityMode {
    fn identity_update_sql(self, table_name: &str, final_large_text: &str) -> String {
        match self {
            Self::UsingIndex => format!(
                "update {table_name} set name = {}, large_text = {} where id = {} and surname = {}",
                quote_literal(UPDATED_NAME_IDENTITY),
                quote_literal(final_large_text),
                INITIAL_ID,
                quote_literal(INITIAL_SURNAME),
            ),
            _ => format!(
                "update {table_name} set surname = {}, large_text = {} where id = {} and surname \
                 = {}",
                quote_literal(UPDATED_SURNAME_IDENTITY),
                quote_literal(final_large_text),
                INITIAL_ID,
                quote_literal(INITIAL_SURNAME),
            ),
        }
    }

    fn delete_sql(self, table_name: &str) -> String {
        match self {
            Self::UsingIndex => format!(
                "delete from {table_name} where id = {} and name = {} and surname = {}",
                INITIAL_ID,
                quote_literal(UPDATED_NAME_IDENTITY),
                quote_literal(INITIAL_SURNAME),
            ),
            Self::Nothing => format!(
                "delete from {table_name} where id = {} and surname = {}",
                INITIAL_ID,
                quote_literal(INITIAL_SURNAME),
            ),
            _ => format!(
                "delete from {table_name} where id = {} and surname = {}",
                INITIAL_ID,
                quote_literal(UPDATED_SURNAME_IDENTITY),
            ),
        }
    }
}

struct ReplicaIdentityScenarioResult {
    events: Vec<Event>,
    non_identity_update: Result<u64, tokio_postgres::Error>,
    toast_update: Result<u64, tokio_postgres::Error>,
    identity_update: Result<u64, tokio_postgres::Error>,
    delete: Result<u64, tokio_postgres::Error>,
    initial_large_text: String,
    updated_large_text: String,
    final_large_text: String,
}

fn generate_random_ascii_string(length: usize) -> String {
    let rng = rand::rng();
    rng.sample_iter(Alphanumeric).take(length).map(char::from).collect()
}

fn data_events(events: Vec<Event>) -> Vec<Event> {
    events
        .into_iter()
        .filter(|event| matches!(event, Event::Insert(_) | Event::Update(_) | Event::Delete(_)))
        .collect()
}

fn find_update_event(events: &[Event], update_index: usize) -> &UpdateEvent {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Update(update) => Some(update),
            _ => None,
        })
        .nth(update_index)
        .expect("expected update event")
}

fn find_delete_event(events: &[Event]) -> &DeleteEvent {
    events
        .iter()
        .find_map(|event| match event {
            Event::Delete(delete) => Some(delete),
            _ => None,
        })
        .expect("expected delete event")
}

fn full_row(name: &str, surname: &str, city: &str, large_text: &str) -> TableRow {
    TableRow::new(vec![
        Cell::I64(INITIAL_ID),
        Cell::String(name.to_owned()),
        Cell::String(surname.to_owned()),
        Cell::String(city.to_owned()),
        Cell::String(large_text.to_owned()),
    ])
}

fn default_identity_row(surname: &str) -> TableRow {
    TableRow::new(vec![Cell::I64(INITIAL_ID), Cell::String(surname.to_owned())])
}

fn using_index_identity_row(name: &str, surname: &str) -> TableRow {
    TableRow::new(vec![Cell::String(name.to_owned()), Cell::String(surname.to_owned())])
}

fn reordered_primary_key_identity_row(first_key: i64, last_key: i64) -> TableRow {
    let mut values = (1_i64..=10).map(Cell::I64).collect::<Vec<_>>();
    values[0] = Cell::I64(first_key);
    values[9] = Cell::I64(last_key);
    TableRow::new(values)
}

async fn run_replica_identity_scenario(
    replica_identity: ReplicaIdentityMode,
) -> ReplicaIdentityScenarioResult {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("replica_identity_composite");
    let table_id = database
        .create_table(
            table_name.clone(),
            false,
            &[
                ("id", "bigint not null"),
                ("name", "text not null"),
                ("surname", "text not null"),
                ("city", "text not null"),
                ("large_text", "text not null"),
            ],
        )
        .await
        .unwrap();

    database
        .run_sql(&format!(
            "alter table {} add primary key (surname, id)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AlterColumn {
                name: "large_text",
                alteration: "set storage external",
            }],
        )
        .await
        .unwrap();

    if matches!(replica_identity, ReplicaIdentityMode::UsingIndex) {
        let index_name = format!("{}_replica_identity_idx", table_name.name);
        database
            .run_sql(&format!(
                "create unique index {} on {} (surname, name)",
                quote_identifier(&index_name),
                table_name.as_quoted_identifier(),
            ))
            .await
            .unwrap();
        let replica_identity_value = format!("using index {}", quote_identifier(&index_name));
        database
            .alter_table(
                table_name.clone(),
                &[TableModification::ReplicaIdentity { value: &replica_identity_value }],
            )
            .await
            .unwrap();
    } else {
        match replica_identity {
            ReplicaIdentityMode::Default => {}
            ReplicaIdentityMode::Full => {
                database
                    .alter_table(
                        table_name.clone(),
                        &[TableModification::ReplicaIdentity { value: "full" }],
                    )
                    .await
                    .unwrap();
            }
            ReplicaIdentityMode::Nothing => {
                database
                    .alter_table(
                        table_name.clone(),
                        &[TableModification::ReplicaIdentity { value: "nothing" }],
                    )
                    .await
                    .unwrap();
            }
            ReplicaIdentityMode::UsingIndex => unreachable!(),
        }
    }

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let publication_name = "test_pub_replica_identity".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();
    table_sync_complete_notify.notified().await;

    let initial_large_text = generate_random_ascii_string(LARGE_TEXT_SIZE_BYTES);
    let updated_large_text = generate_random_ascii_string(LARGE_TEXT_SIZE_BYTES);
    let final_large_text = generate_random_ascii_string(LARGE_TEXT_SIZE_BYTES);

    let insert_event_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;
    database
        .insert_values(
            table_name.clone(),
            &["id", "name", "surname", "city", "large_text"],
            &[&INITIAL_ID, &INITIAL_NAME, &INITIAL_SURNAME, &INITIAL_CITY, &initial_large_text],
        )
        .await
        .unwrap();
    insert_event_notify.notified().await;

    let mut update_count = 0;

    let non_identity_update_sql = format!(
        "update {} set city = {} where id = {} and surname = {}",
        table_name.as_quoted_identifier(),
        quote_literal(UPDATED_CITY),
        INITIAL_ID,
        quote_literal(INITIAL_SURNAME),
    );
    let non_identity_update_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(
            EventType::Update,
            table_id,
            update_count + 1,
        )])
        .await;
    let non_identity_update = database.run_sql(&non_identity_update_sql).await;
    if non_identity_update.is_ok() {
        non_identity_update_notify.notified().await;
        update_count += 1;
    }

    let toast_update_sql = format!(
        "update {} set large_text = {} where id = {} and surname = {}",
        table_name.as_quoted_identifier(),
        quote_literal(&updated_large_text),
        INITIAL_ID,
        quote_literal(INITIAL_SURNAME),
    );
    let toast_update_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(
            EventType::Update,
            table_id,
            update_count + 1,
        )])
        .await;
    let toast_update = database.run_sql(&toast_update_sql).await;
    if toast_update.is_ok() {
        toast_update_notify.notified().await;
        update_count += 1;
    }

    let identity_update_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(
            EventType::Update,
            table_id,
            update_count + 1,
        )])
        .await;
    let identity_update = database
        .run_sql(
            &replica_identity
                .identity_update_sql(&table_name.as_quoted_identifier(), &final_large_text),
        )
        .await;
    if identity_update.is_ok() {
        identity_update_notify.notified().await;
    }

    let delete_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Delete, table_id, 1)])
        .await;
    let delete =
        database.run_sql(&replica_identity.delete_sql(&table_name.as_quoted_identifier())).await;
    if delete.is_ok() {
        delete_notify.notified().await;
    }

    pipeline.shutdown_and_wait().await.unwrap();

    ReplicaIdentityScenarioResult {
        events: destination.get_events().await,
        non_identity_update,
        toast_update,
        identity_update,
        delete,
        initial_large_text,
        updated_large_text,
        final_large_text,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn default_replica_identity_with_composite_primary_key_handles_partial_and_key_rows() {
    let result = run_replica_identity_scenario(ReplicaIdentityMode::Default).await;

    assert!(result.non_identity_update.is_ok());
    assert!(result.toast_update.is_ok());
    assert!(result.identity_update.is_ok());
    assert!(result.delete.is_ok());

    let events = data_events(result.events);
    assert_eq!(events.len(), 5);

    assert!(matches!(
        &events[0],
        Event::Insert(insert) if insert.table_row
            == full_row(INITIAL_NAME, INITIAL_SURNAME, INITIAL_CITY, &result.initial_large_text)
    ));

    let non_identity_update = find_update_event(&events, 0);
    assert_eq!(
        non_identity_update.updated_table_row,
        UpdatedTableRow::Partial(PartialTableRow::new(
            5,
            TableRow::new(vec![
                Cell::I64(INITIAL_ID),
                Cell::String(INITIAL_NAME.to_owned()),
                Cell::String(INITIAL_SURNAME.to_owned()),
                Cell::String(UPDATED_CITY.to_owned()),
            ]),
            vec![4],
        ))
    );
    assert_eq!(non_identity_update.old_table_row, None);

    let toast_update = find_update_event(&events, 1);
    assert_eq!(
        toast_update.updated_table_row,
        UpdatedTableRow::Full(full_row(
            INITIAL_NAME,
            INITIAL_SURNAME,
            UPDATED_CITY,
            &result.updated_large_text,
        ))
    );
    assert_eq!(toast_update.old_table_row, None);

    let identity_update = find_update_event(&events, 2);
    assert_eq!(
        identity_update.updated_table_row,
        UpdatedTableRow::Full(full_row(
            INITIAL_NAME,
            UPDATED_SURNAME_IDENTITY,
            UPDATED_CITY,
            &result.final_large_text,
        ))
    );
    assert_eq!(
        identity_update.old_table_row,
        Some(OldTableRow::Key(default_identity_row(INITIAL_SURNAME)))
    );

    let delete = find_delete_event(&events);
    assert_eq!(
        delete.old_table_row,
        Some(OldTableRow::Key(default_identity_row(UPDATED_SURNAME_IDENTITY)))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn full_replica_identity_with_composite_primary_key_preserves_full_old_rows() {
    let result = run_replica_identity_scenario(ReplicaIdentityMode::Full).await;

    assert!(result.non_identity_update.is_ok());
    assert!(result.toast_update.is_ok());
    assert!(result.identity_update.is_ok());
    assert!(result.delete.is_ok());

    let events = data_events(result.events);
    assert_eq!(events.len(), 5);

    let non_identity_update = find_update_event(&events, 0);
    assert_eq!(
        non_identity_update.updated_table_row,
        UpdatedTableRow::Full(full_row(
            INITIAL_NAME,
            INITIAL_SURNAME,
            UPDATED_CITY,
            &result.initial_large_text,
        ))
    );
    assert_eq!(
        non_identity_update.old_table_row,
        Some(OldTableRow::Full(full_row(
            INITIAL_NAME,
            INITIAL_SURNAME,
            INITIAL_CITY,
            &result.initial_large_text,
        )))
    );

    let toast_update = find_update_event(&events, 1);
    assert_eq!(
        toast_update.updated_table_row,
        UpdatedTableRow::Full(full_row(
            INITIAL_NAME,
            INITIAL_SURNAME,
            UPDATED_CITY,
            &result.updated_large_text,
        ))
    );
    assert_eq!(
        toast_update.old_table_row,
        Some(OldTableRow::Full(full_row(
            INITIAL_NAME,
            INITIAL_SURNAME,
            UPDATED_CITY,
            &result.initial_large_text,
        )))
    );

    let identity_update = find_update_event(&events, 2);
    assert_eq!(
        identity_update.updated_table_row,
        UpdatedTableRow::Full(full_row(
            INITIAL_NAME,
            UPDATED_SURNAME_IDENTITY,
            UPDATED_CITY,
            &result.final_large_text,
        ))
    );
    assert_eq!(
        identity_update.old_table_row,
        Some(OldTableRow::Full(full_row(
            INITIAL_NAME,
            INITIAL_SURNAME,
            UPDATED_CITY,
            &result.updated_large_text,
        )))
    );

    let delete = find_delete_event(&events);
    assert_eq!(
        delete.old_table_row,
        Some(OldTableRow::Full(full_row(
            INITIAL_NAME,
            UPDATED_SURNAME_IDENTITY,
            UPDATED_CITY,
            &result.final_large_text,
        )))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn using_index_replica_identity_keeps_key_rows_in_table_order() {
    let result = run_replica_identity_scenario(ReplicaIdentityMode::UsingIndex).await;

    assert!(result.non_identity_update.is_ok());
    assert!(result.toast_update.is_ok());
    assert!(result.identity_update.is_ok());
    assert!(result.delete.is_ok());

    let events = data_events(result.events);
    assert_eq!(events.len(), 5);

    let non_identity_update = find_update_event(&events, 0);
    assert_eq!(
        non_identity_update.updated_table_row,
        UpdatedTableRow::Partial(PartialTableRow::new(
            5,
            TableRow::new(vec![
                Cell::I64(INITIAL_ID),
                Cell::String(INITIAL_NAME.to_owned()),
                Cell::String(INITIAL_SURNAME.to_owned()),
                Cell::String(UPDATED_CITY.to_owned()),
            ]),
            vec![4],
        ))
    );
    assert_eq!(non_identity_update.old_table_row, None);

    let toast_update = find_update_event(&events, 1);
    assert_eq!(
        toast_update.updated_table_row,
        UpdatedTableRow::Full(full_row(
            INITIAL_NAME,
            INITIAL_SURNAME,
            UPDATED_CITY,
            &result.updated_large_text,
        ))
    );
    assert_eq!(toast_update.old_table_row, None);

    let identity_update = find_update_event(&events, 2);
    assert_eq!(
        identity_update.updated_table_row,
        UpdatedTableRow::Full(full_row(
            UPDATED_NAME_IDENTITY,
            INITIAL_SURNAME,
            UPDATED_CITY,
            &result.final_large_text,
        ))
    );
    assert_eq!(
        identity_update.old_table_row,
        Some(OldTableRow::Key(using_index_identity_row(INITIAL_NAME, INITIAL_SURNAME,)))
    );

    let delete = find_delete_event(&events);
    assert_eq!(
        delete.old_table_row,
        Some(OldTableRow::Key(using_index_identity_row(UPDATED_NAME_IDENTITY, INITIAL_SURNAME,)))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn using_index_with_reordered_primary_key_columns_remains_primary_key_identity() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("reordered_primary_key_identity");
    let quoted_table_name = table_name.as_quoted_identifier();
    let table_id = database
        .create_table(
            table_name.clone(),
            false,
            &[
                ("key_01", "bigint not null"),
                ("key_02", "bigint not null"),
                ("key_03", "bigint not null"),
                ("key_04", "bigint not null"),
                ("key_05", "bigint not null"),
                ("key_06", "bigint not null"),
                ("key_07", "bigint not null"),
                ("key_08", "bigint not null"),
                ("key_09", "bigint not null"),
                ("key_10", "bigint not null"),
                ("payload", "text not null"),
            ],
        )
        .await
        .unwrap();

    let primary_key_columns =
        "key_01, key_02, key_03, key_04, key_05, key_06, key_07, key_08, key_09, key_10";
    database
        .run_sql(&format!(
            "alter table {quoted_table_name} add primary key ({primary_key_columns})"
        ))
        .await
        .unwrap();

    let index_name = format!("{}_identity_idx", table_name.name);
    database
        .run_sql(&format!(
            "create unique index {} on {quoted_table_name} (key_10, key_09, key_08, key_07, \
             key_06, key_05, key_04, key_03, key_02, key_01)",
            quote_identifier(&index_name),
        ))
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let publication_name = "test_pub_reordered_primary_key_identity".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let mut pipeline = create_pipeline(
        &database.config,
        random(),
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;

    let insert_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    database
        .run_sql(&format!(
            "insert into {quoted_table_name} ({primary_key_columns}, payload) values (1, 2, 3, 4, \
             5, 6, 7, 8, 9, 10, 'initial')"
        ))
        .await
        .unwrap();

    insert_notify.notified().await;

    let default_update_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Update, table_id, 1)])
        .await;

    database
        .run_sql(&format!(
            "update {quoted_table_name} set key_01 = 101, payload = 'default' where key_01 = 1"
        ))
        .await
        .unwrap();

    default_update_notify.notified().await;

    let relation_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Relation, table_id, 2)])
        .await;
    let index_update_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Update, table_id, 2)])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::ReplicaIdentity {
                value: &format!("using index {}", quote_identifier(&index_name)),
            }],
        )
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "update {quoted_table_name} set key_10 = 110, payload = 'using index' where key_01 = \
             101"
        ))
        .await
        .unwrap();

    relation_notify.notified().await;
    index_update_notify.notified().await;

    let delete_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Delete, table_id, 1)])
        .await;

    database.run_sql(&format!("delete from {quoted_table_name} where key_01 = 101")).await.unwrap();

    delete_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let relation_schemas = events
        .iter()
        .filter_map(|event| match event {
            Event::Relation(relation) if relation.replicated_table_schema.id() == table_id => {
                Some(&relation.replicated_table_schema)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let [default_relation_schema, using_index_relation_schema] =
        relation_schemas.get(relation_schemas.len().saturating_sub(2)..).unwrap()
    else {
        panic!("expected two relation schemas");
    };

    // The identity DDL creates a new snapshot, but does not change the effective
    // replicated schema.
    assert!(
        using_index_relation_schema.inner().snapshot_id
            > default_relation_schema.inner().snapshot_id
    );
    assert_eq!(
        (
            default_relation_schema.id(),
            default_relation_schema.name(),
            default_relation_schema.inner().column_schemas.as_slice(),
            default_relation_schema.replication_mask(),
            default_relation_schema.identity_mask(),
            default_relation_schema.identity_type(),
        ),
        (
            using_index_relation_schema.id(),
            using_index_relation_schema.name(),
            using_index_relation_schema.inner().column_schemas.as_slice(),
            using_index_relation_schema.replication_mask(),
            using_index_relation_schema.identity_mask(),
            using_index_relation_schema.identity_type(),
        )
    );

    let events = data_events(events);
    assert_eq!(events.len(), 4);

    let default_update = find_update_event(&events, 0);
    assert_eq!(
        default_update.old_table_row,
        Some(OldTableRow::Key(reordered_primary_key_identity_row(1, 10)))
    );

    let index_update = find_update_event(&events, 1);
    assert_eq!(
        index_update.old_table_row,
        Some(OldTableRow::Key(reordered_primary_key_identity_row(101, 10)))
    );

    let delete = find_delete_event(&events);
    assert_eq!(
        delete.old_table_row,
        Some(OldTableRow::Key(reordered_primary_key_identity_row(101, 110)))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn none_replica_identity_with_composite_primary_key_rejects_updates_and_deletes() {
    let result = run_replica_identity_scenario(ReplicaIdentityMode::Nothing).await;

    assert!(result.non_identity_update.is_err());
    assert!(result.toast_update.is_err());
    assert!(result.identity_update.is_err());
    assert!(result.delete.is_err());

    let events = data_events(result.events);
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], Event::Insert(_)));
}
