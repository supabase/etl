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
    for (event_type, count) in conditions {
        let Some(inner_events) = grouped_events.get(&event_type) else {
            return false;
        };

        if inner_events.len() != count as usize {
            return false;
        }
    }

    true
}
