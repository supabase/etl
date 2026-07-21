use std::collections::HashMap;

use crate::{
    data::TableRow,
    event::{Event, EventType},
    schema::{ReplicatedTableSchema, TableId},
};

/// Condition for waiting on events in tests.
#[derive(Clone, Debug)]
pub enum EventCondition {
    /// Wait for an exact count of events of the given type across all tables.
    AnyCount(EventType, u64),
    /// Wait for an exact count of events of the given type for a table.
    TableCount(EventType, TableId, u64),
}

/// Checks whether the recorded events satisfy every condition.
pub fn check_event_conditions(events: &[Event], conditions: &[EventCondition]) -> bool {
    let grouped_events_by_type = group_events_by_type(events);
    let grouped_events_by_type_and_table = group_events_by_type_and_table_id(events);

    conditions.iter().all(|condition| match condition {
        EventCondition::AnyCount(event_type, expected_count) => {
            grouped_events_by_type.get(event_type).map_or(0, |events| events.len() as u64)
                == *expected_count
        }
        EventCondition::TableCount(event_type, table_id, expected_count) => {
            grouped_events_by_type_and_table
                .get(&(event_type.clone(), *table_id))
                .map_or(0, |events| events.len() as u64)
                == *expected_count
        }
    })
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

/// Checks if the combined count of events and table rows equals the expected
/// counts.
///
/// Supports two condition types:
/// - [`EventCondition::AnyCount`]: counts events across all tables
/// - [`EventCondition::TableCount`]: counts events for a specific table only
///
/// For [`EventType::Insert`], both streaming insert events and table copy rows
/// are counted. For other event types, only streaming events are counted.
pub fn check_all_event_conditions(
    events: &[Event],
    table_rows: &HashMap<TableId, Vec<TableRow>>,
    conditions: &[EventCondition],
) -> bool {
    let grouped_events_by_type = group_events_by_type(events);
    let grouped_events_by_type_and_table = group_events_by_type_and_table_id(events);

    conditions.iter().all(|condition| match condition {
        EventCondition::AnyCount(event_type, expected_count) => {
            let event_count =
                grouped_events_by_type.get(event_type).map_or(0, |events| events.len() as u64);

            let table_row_count = if *event_type == EventType::Insert {
                table_rows.values().map(|rows| rows.len() as u64).sum()
            } else {
                0
            };

            event_count + table_row_count == *expected_count
        }
        EventCondition::TableCount(event_type, table_id, expected_count) => {
            let event_count = grouped_events_by_type_and_table
                .get(&(event_type.clone(), *table_id))
                .map_or(0, |events| events.len() as u64);

            let table_row_count = if *event_type == EventType::Insert {
                table_rows.get(table_id).map_or(0, |rows| rows.len() as u64)
            } else {
                0
            };

            event_count + table_row_count == *expected_count
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_conditions_use_exact_counts() {
        let events = vec![Event::Unsupported, Event::Unsupported];

        assert!(check_event_conditions(
            &events,
            &[EventCondition::AnyCount(EventType::Unsupported, 2)]
        ));
        assert!(!check_event_conditions(
            &events,
            &[EventCondition::AnyCount(EventType::Unsupported, 1)]
        ));
        assert!(!check_event_conditions(
            &events,
            &[EventCondition::AnyCount(EventType::Unsupported, 3)]
        ));
    }

    #[test]
    fn all_event_conditions_treat_copied_rows_as_inserts() {
        let table_id = TableId::new(1);
        let table_rows =
            HashMap::from([(table_id, vec![TableRow::new(Vec::new()), TableRow::new(Vec::new())])]);

        assert!(check_all_event_conditions(
            &[],
            &table_rows,
            &[
                EventCondition::AnyCount(EventType::Insert, 2),
                EventCondition::TableCount(EventType::Insert, table_id, 2),
            ],
        ));
        assert!(!check_all_event_conditions(
            &[],
            &table_rows,
            &[EventCondition::TableCount(EventType::Insert, table_id, 1)],
        ));
        assert!(!check_event_conditions(
            &[],
            &[EventCondition::TableCount(EventType::Insert, table_id, 1)],
        ));
    }
}
