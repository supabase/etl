use etl_postgres::schema::TableId;

use crate::types::{Event, TableRow};

/// Trait for converting a TableRow into a domain-specific struct
pub trait FromTableRow: Sized {
    /// The type used to identify this record (usually an ID field)
    type Id: Eq + Clone;

    /// Convert a TableRow into this type, returning None if conversion fails
    fn from_table_row(table_row: &TableRow) -> Option<Self>;

    /// Get the identifier for this record (used for updates/deletes)
    fn id(&self) -> Self::Id;
}

/// Materializes events from a MemoryDestination into a collection of typed records
///
/// This function processes Insert/Update/Delete events sequentially:
/// - Insert: Add new record to collection
/// - Update: Replace existing record with same ID, or add if not found
/// - Delete: Remove record with matching ID
///
/// # Arguments
/// * `destination` - The test destination wrapper containing events
/// * `table_id` - Optional table ID to filter events (if None, processes all tables)
///
/// # Returns
/// A vector containing the final state after all events are applied
pub async fn materialize_events<T>(events: &[Event], table_id: Option<TableId>) -> Vec<T>
where
    T: FromTableRow,
{
    let mut records: Vec<T> = Vec::new();

    for event in events {
        // Filter by table_id if specified
        if let Some(target_table_id) = table_id {
            let event_table_id = match &event {
                Event::Insert(insert_event) => insert_event.table_id,
                Event::Update(update_event) => update_event.table_id,
                Event::Delete(delete_event) => delete_event.table_id,
                _ => continue, // Skip other event types
            };

            if event_table_id != target_table_id {
                continue;
            }
        }

        match event {
            Event::Insert(insert_event) => {
                if let Some(record) = T::from_table_row(&insert_event.table_row) {
                    records.push(record);
                }
            }
            Event::Update(update_event) => {
                if let Some(new_record) = T::from_table_row(&update_event.table_row) {
                    let new_id = new_record.id();

                    // Find and replace existing record with same ID
                    if let Some(pos) = records.iter().position(|r| r.id() == new_id) {
                        records[pos] = new_record;
                    } else {
                        // If no existing record found, add as new
                        records.push(new_record);
                    }
                }
            }
            Event::Delete(delete_event) => {
                if let Some((_, old_table_row)) = &delete_event.old_table_row
                    && let Some(old_record) = T::from_table_row(old_table_row)
                {
                    let delete_id = old_record.id();
                    records.retain(|r| r.id() != delete_id);
                }
            }
            _ => {
                // Ignore other event types (Truncate, etc.)
            }
        }
    }

    records
}
