use crate::types::{Event, EventType, TableRow};
use etl_postgres::types::TableId;
use std::collections::HashMap;

pub fn group_events_by_type(events: &[Event]) -> HashMap<EventType, Vec<Event>> {
    let mut grouped = HashMap::new();
    for event in events {
        let event_type = EventType::from(event);
        grouped
            .entry(event_type)
            .or_insert_with(Vec::new)
            .push(event.clone());
    }

    grouped
}

pub fn group_events_by_type_and_table_id(
    events: &[Event],
) -> HashMap<(EventType, TableId), Vec<Event>> {
    let mut grouped = HashMap::new();
    for event in events {
        let event_type = EventType::from(event);
        // This grouping works on DML operations and Relation events.
        let table_ids = match event {
            Event::Relation(event) => vec![event.replicated_table_schema.id()],
            Event::Insert(event) => vec![event.replicated_table_schema.id()],
            Event::Update(event) => vec![event.replicated_table_schema.id()],
            Event::Delete(event) => vec![event.replicated_table_schema.id()],
            Event::Truncate(event) => event
                .truncated_tables
                .iter()
                .map(|schema| schema.id())
                .collect(),
            _ => vec![],
        };
        for table_id in table_ids {
            grouped
                .entry((event_type.clone(), table_id))
                .or_insert_with(Vec::new)
                .push(event.clone());
        }
    }

    grouped
}

pub fn check_events_count(events: &[Event], conditions: Vec<(EventType, u64)>) -> bool {
    let grouped_events = group_events_by_type(events);

    conditions.into_iter().all(|(event_type, count)| {
        grouped_events
            .get(&event_type)
            .map(|inner| inner.len() == count as usize)
            .unwrap_or(false)
    })
}

/// Compares two events for equality in test contexts.
///
/// This function compares events based on their key fields, ignoring LSN values since those
/// may vary between pipeline runs.
fn events_equal(a: &Event, b: &Event) -> bool {
    match (a, b) {
        (Event::Begin(a), Event::Begin(b)) => a == b,
        (Event::Commit(a), Event::Commit(b)) => a == b,
        (Event::Truncate(a), Event::Truncate(b)) => {
            if a.options != b.options || a.truncated_tables.len() != b.truncated_tables.len() {
                return false;
            }

            // Compare table IDs of truncated tables
            let a_ids: Vec<_> = a.truncated_tables.iter().map(|s| s.id()).collect();
            let b_ids: Vec<_> = b.truncated_tables.iter().map(|s| s.id()).collect();

            a_ids == b_ids
        }
        (Event::Insert(a), Event::Insert(b)) => {
            a.replicated_table_schema.id() == b.replicated_table_schema.id()
                && a.table_row == b.table_row
        }
        (Event::Update(a), Event::Update(b)) => {
            a.replicated_table_schema.id() == b.replicated_table_schema.id()
                && a.table_row == b.table_row
                && a.old_table_row == b.old_table_row
        }
        (Event::Delete(a), Event::Delete(b)) => {
            a.replicated_table_schema.id() == b.replicated_table_schema.id()
                && a.old_table_row == b.old_table_row
        }
        (Event::Unsupported, Event::Unsupported) => true,
        _ => false,
    }
}

/// Checks if the combined count of events and table rows meets the expected counts across all tables.
///
/// This function groups events once for efficient lookup, then checks each condition
/// to see if the sum of streaming events and copied table rows meets or exceeds the
/// expected count.
///
/// For [`EventType::Insert`], both streaming insert events and table copy rows are counted.
/// For other event types, only streaming events are counted.
pub fn check_all_events_count(
    events: &[Event],
    table_rows: &HashMap<TableId, Vec<TableRow>>,
    conditions: Vec<(EventType, u64)>,
) -> bool {
    let grouped_events = group_events_by_type(events);

    conditions.iter().all(|(event_type, expected_count)| {
        // Count events of the specified type across all tables.
        let event_count = grouped_events
            .get(event_type)
            .map(|events| events.len() as u64)
            .unwrap_or(0);

        // Count all table rows (treated as inserts) across all tables.
        let table_row_count = if *event_type == EventType::Insert {
            table_rows.values().map(|rows| rows.len() as u64).sum()
        } else {
            0
        };

        let total = event_count + table_row_count;
        total >= *expected_count
    })
}

/// Returns a new Vec of events with duplicates removed.
///
/// Row-level events (Insert, Update, Delete) are de-duplicated by comparing key fields,
/// ignoring LSN values that may vary between pipeline runs. Non-row events
/// (Begin, Commit, Relation, Truncate, Unsupported) are also de-duplicated using the
/// same equality comparison.
///
/// The first occurrence of each event is kept and subsequent duplicates are dropped.
///
/// The rationale for having this method is that the pipeline doesn't guarantee exactly once delivery,
/// thus in some tests we might have to exclude duplicates while performing assertions.
pub fn deduplicate_events(events: &[Event]) -> Vec<Event> {
    let mut result: Vec<Event> = Vec::with_capacity(events.len());

    for event in events.iter().cloned() {
        if !result.iter().any(|existing| events_equal(existing, &event)) {
            result.push(event);
        }
    }

    result
}
