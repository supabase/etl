use etl::{
    state::table::TableReplicationPhaseType,
    test_utils::{
        database::{spawn_source_database, test_table_name},
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
    },
    types::{
        Cell, Event, EventType, OldTableRow, PartialTableRow, PipelineId, TableRow, UpdatedTableRow,
    },
};
use etl_postgres::tokio::test_utils::TableModification;
use etl_telemetry::tracing::init_test_tracing;
use pg_escape::quote_literal;
use rand::{Rng, distr::Alphanumeric, random};

const LARGE_TEXT_SIZE_BYTES: usize = 8192;
const INITIAL_ID: i64 = 1;
const INITIAL_NAME: &str = "alice";
const INITIAL_SURNAME: &str = "smith";
const UPDATED_NAME: &str = "alicia";
const UPDATED_SURNAME_IDENTITY: &str = "smithers";

#[derive(Clone, Copy)]
enum ReplicaIdentityMode {
    Default,
    Full,
    Nothing,
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

fn find_update_event(events: &[Event], update_index: usize) -> &etl::types::UpdateEvent {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Update(update) => Some(update),
            _ => None,
        })
        .nth(update_index)
        .expect("expected update event")
}

fn find_delete_event(events: &[Event]) -> &etl::types::DeleteEvent {
    events
        .iter()
        .find_map(|event| match event {
            Event::Delete(delete) => Some(delete),
            _ => None,
        })
        .expect("expected delete event")
}

fn composite_key_row_in_table_order(second_value: &str) -> TableRow {
    TableRow::new(vec![Cell::I64(INITIAL_ID), Cell::String(second_value.to_string())])
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
    }

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let publication_name = "test_pub_replica_identity".to_string();
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

    let table_ready_notify =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();
    table_ready_notify.notified().await;

    let initial_large_text = generate_random_ascii_string(LARGE_TEXT_SIZE_BYTES);
    let updated_large_text = generate_random_ascii_string(LARGE_TEXT_SIZE_BYTES);
    let final_large_text = generate_random_ascii_string(LARGE_TEXT_SIZE_BYTES);

    let insert_event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;
    database
        .insert_values(
            table_name.clone(),
            &["id", "name", "surname", "large_text"],
            &[&INITIAL_ID, &INITIAL_NAME, &INITIAL_SURNAME, &initial_large_text],
        )
        .await
        .unwrap();
    insert_event_notify.notified().await;

    let mut update_count = 0;

    let non_identity_update_sql = format!(
        "update {} set name = {} where id = {} and surname = {}",
        table_name.as_quoted_identifier(),
        quote_literal(UPDATED_NAME),
        INITIAL_ID,
        quote_literal(INITIAL_SURNAME),
    );
    let non_identity_update_notify =
        destination.wait_for_events_count(vec![(EventType::Update, update_count + 1)]).await;
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
    let toast_update_notify =
        destination.wait_for_events_count(vec![(EventType::Update, update_count + 1)]).await;
    let toast_update = database.run_sql(&toast_update_sql).await;
    if toast_update.is_ok() {
        toast_update_notify.notified().await;
        update_count += 1;
    }

    let identity_update_sql = format!(
        "update {} set surname = {}, large_text = {} where id = {} and surname = {}",
        table_name.as_quoted_identifier(),
        quote_literal(UPDATED_SURNAME_IDENTITY),
        quote_literal(&final_large_text),
        INITIAL_ID,
        quote_literal(INITIAL_SURNAME),
    );
    let identity_update_notify =
        destination.wait_for_events_count(vec![(EventType::Update, update_count + 1)]).await;
    let identity_update = database.run_sql(&identity_update_sql).await;
    if identity_update.is_ok() {
        identity_update_notify.notified().await;
    }

    let delete_sql = format!(
        "delete from {} where id = {} and surname = {}",
        table_name.as_quoted_identifier(),
        INITIAL_ID,
        quote_literal(match replica_identity {
            ReplicaIdentityMode::Nothing => INITIAL_SURNAME,
            _ => UPDATED_SURNAME_IDENTITY,
        }),
    );
    let delete_notify = destination.wait_for_events_count(vec![(EventType::Delete, 1)]).await;
    let delete = database.run_sql(&delete_sql).await;
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
        Event::Insert(insert)
            if insert.table_row
                == TableRow::new(vec![
                    Cell::I64(INITIAL_ID),
                    Cell::String(INITIAL_NAME.to_string()),
                    Cell::String(INITIAL_SURNAME.to_string()),
                    Cell::String(result.initial_large_text.clone()),
                ])
    ));

    let non_identity_update = find_update_event(&events, 0);
    assert_eq!(
        non_identity_update.updated_table_row,
        UpdatedTableRow::Partial(PartialTableRow::new(
            4,
            TableRow::new(vec![
                Cell::I64(INITIAL_ID),
                Cell::String(UPDATED_NAME.to_string()),
                Cell::String(INITIAL_SURNAME.to_string()),
            ]),
            vec![3],
        ))
    );
    assert_eq!(non_identity_update.old_table_row, None);

    let toast_update = find_update_event(&events, 1);
    assert_eq!(
        toast_update.updated_table_row,
        UpdatedTableRow::Full(TableRow::new(vec![
            Cell::I64(INITIAL_ID),
            Cell::String(UPDATED_NAME.to_string()),
            Cell::String(INITIAL_SURNAME.to_string()),
            Cell::String(result.updated_large_text.clone()),
        ]))
    );
    assert_eq!(toast_update.old_table_row, None);

    let identity_update = find_update_event(&events, 2);
    assert_eq!(
        identity_update.updated_table_row,
        UpdatedTableRow::Full(TableRow::new(vec![
            Cell::I64(INITIAL_ID),
            Cell::String(UPDATED_NAME.to_string()),
            Cell::String(UPDATED_SURNAME_IDENTITY.to_string()),
            Cell::String(result.final_large_text.clone()),
        ]))
    );
    assert_eq!(
        identity_update.old_table_row,
        Some(OldTableRow::Key(composite_key_row_in_table_order(INITIAL_SURNAME)))
    );

    let delete = find_delete_event(&events);
    assert_eq!(
        delete.old_table_row,
        Some(OldTableRow::Key(composite_key_row_in_table_order(UPDATED_SURNAME_IDENTITY)))
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
        UpdatedTableRow::Full(TableRow::new(vec![
            Cell::I64(INITIAL_ID),
            Cell::String(UPDATED_NAME.to_string()),
            Cell::String(INITIAL_SURNAME.to_string()),
            Cell::String(result.initial_large_text.clone()),
        ]))
    );
    assert_eq!(
        non_identity_update.old_table_row,
        Some(OldTableRow::Full(TableRow::new(vec![
            Cell::I64(INITIAL_ID),
            Cell::String(INITIAL_NAME.to_string()),
            Cell::String(INITIAL_SURNAME.to_string()),
            Cell::String(result.initial_large_text.clone()),
        ])))
    );

    let toast_update = find_update_event(&events, 1);
    assert_eq!(
        toast_update.old_table_row,
        Some(OldTableRow::Full(TableRow::new(vec![
            Cell::I64(INITIAL_ID),
            Cell::String(UPDATED_NAME.to_string()),
            Cell::String(INITIAL_SURNAME.to_string()),
            Cell::String(result.initial_large_text.clone()),
        ])))
    );

    let identity_update = find_update_event(&events, 2);
    assert_eq!(
        identity_update.old_table_row,
        Some(OldTableRow::Full(TableRow::new(vec![
            Cell::I64(INITIAL_ID),
            Cell::String(UPDATED_NAME.to_string()),
            Cell::String(INITIAL_SURNAME.to_string()),
            Cell::String(result.updated_large_text.clone()),
        ])))
    );

    let delete = find_delete_event(&events);
    assert_eq!(
        delete.old_table_row,
        Some(OldTableRow::Full(TableRow::new(vec![
            Cell::I64(INITIAL_ID),
            Cell::String(UPDATED_NAME.to_string()),
            Cell::String(UPDATED_SURNAME_IDENTITY.to_string()),
            Cell::String(result.final_large_text.clone()),
        ])))
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
