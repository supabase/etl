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
        let table_id = match event {
            Event::Insert(event) => Some(event.table_id),
            Event::Update(event) => Some(event.table_id),
            Event::Delete(event) => Some(event.table_id),
            _ => None,
        };
        if let Some(table_id) = table_id {
            grouped
                .entry((event_type, table_id))
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
        if !result.contains(&e) {
            result.push(e);
        }
    }
    result
}
