use std::collections::HashMap;

use etl_postgres::types::{ReplicatedTableSchema, TableId};

use crate::types::{Event, EventType, TableRow};

/// Condition for waiting on events in tests.
#[derive(Clone, Debug)]
pub enum EventCondition {
    /// Wait for a count of events of the given type across all tables.
    Any(EventType, u64),
    /// Wait for a count of events of the given type for a specific table.
    Table(EventType, TableId, u64),
}

pub fn group_events_by_type(events: &[Event]) -> HashMap<EventType, Vec<Event>> {
    let mut grouped = HashMap::new();
    for event in events {
        let event_type = EventType::from(event);
        grouped.entry(event_type).or_insert_with(Vec::new).push(event.clone());
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
            Event::Truncate(event) => {
                event.truncated_tables.iter().map(ReplicatedTableSchema::id).collect()
            }
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

/// Checks if the combined count of events and table rows meets the expected
/// counts.
///
/// Supports two condition types:
/// - [`EventCondition::Any`]: counts events across all tables
/// - [`EventCondition::Table`]: counts events for a specific table only
///
/// For [`EventType::Insert`], both streaming insert events and table copy rows
/// are counted. For other event types, only streaming events are counted.
pub fn check_all_events_count(
    events: &[Event],
    table_rows: &HashMap<TableId, Vec<TableRow>>,
    conditions: Vec<EventCondition>,
) -> bool {
    let grouped_events_by_type = group_events_by_type(events);
    let grouped_events_by_type_and_table = group_events_by_type_and_table_id(events);

    conditions.iter().all(|condition| match condition {
        EventCondition::Any(event_type, expected_count) => {
            let event_count =
                grouped_events_by_type.get(event_type).map_or(0, |events| events.len() as u64);

            let table_row_count = if *event_type == EventType::Insert {
                table_rows.values().map(|rows| rows.len() as u64).sum()
            } else {
                0
            };

            event_count + table_row_count >= *expected_count
        }
        EventCondition::Table(event_type, table_id, expected_count) => {
            let event_count = grouped_events_by_type_and_table
                .get(&(event_type.clone(), *table_id))
                .map_or(0, |events| events.len() as u64);

            let table_row_count = if *event_type == EventType::Insert {
                table_rows.get(table_id).map_or(0, |rows| rows.len() as u64)
            } else {
                0
            };

            event_count + table_row_count >= *expected_count
        }
    })
}
