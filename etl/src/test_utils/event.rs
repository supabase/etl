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
            Event::Insert(event) => vec![event.table_id],
            Event::Update(event) => vec![event.table_id],
            Event::Delete(event) => vec![event.table_id],
            Event::Truncate(event) => event
                .rel_ids
                .iter()
                .map(|rel_id| TableId::new(*rel_id))
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

/// Checks if the combined count of events and table rows meets the expected counts.
///
/// This function groups events once for efficient lookup, then checks each condition
/// to see if the sum of streaming events and copied table rows meets or exceeds the
/// expected count.
///
/// For [`EventType::Insert`], both streaming insert events and table copy rows are counted.
/// For other event types, only streaming events are counted.
pub fn check_all_events_count(
    events: &[Event],
    table_rows: &HashMap<TableId, Vec<crate::types::TableRow>>,
    conditions: Vec<(EventType, TableId, u64)>,
) -> bool {
    let grouped_events = group_events_by_type_and_table_id(events);

    conditions
        .iter()
        .all(|(event_type, table_id, expected_count)| {
            // Count events of the specified type for this table using the grouped map.
            let event_count = grouped_events
                .get(&(event_type.clone(), *table_id))
                .map(|events| events.len() as u64)
                .unwrap_or(0);

            // Count table rows for this table (treated as inserts).
            let table_row_count = if *event_type == EventType::Insert {
                table_rows
                    .get(table_id)
                    .map(|rows| rows.len() as u64)
                    .unwrap_or(0)
            } else {
                0
            };

            let total = event_count + table_row_count;
            total >= *expected_count
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
