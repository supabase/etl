use crate::types::{Event, EventType};
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
        // This grouping only works on simple DML operations.
        let table_ids = match event {
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

/// Returns a new Vec of events with duplicates removed.
///
/// Events that are not tied to a specific row (Begin/Commit/Relation/Truncate/Unsupported)
/// are not de-duplicated and are preserved in order.
/// Returns a new Vec of events with duplicates removed based on full equality of events.
///
/// Two events are considered the same if all their fields are equal. The first
/// occurrence is kept and subsequent duplicates are dropped.
///
/// The rationale for having this method is that the pipeline doesn't guarantee exactly once delivery
/// thus in some tests we might have to exclude duplicates while performing assertions.
pub fn deduplicate_events(events: &[Event]) -> Vec<Event> {
    let mut result: Vec<Event> = Vec::with_capacity(events.len());
    for e in events.iter().cloned() {
        if !result.iter().any(|existing| events_equal(existing, &e)) {
            result.push(e);
        }
    }
    result
}
