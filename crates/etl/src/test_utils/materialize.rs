//! Destination-state oracle for replicated table histories.
//!
//! [`materialize_events`] applies copied rows and CDC events the way a keyed
//! destination would, so tests can compare the result with the committed
//! source table. Pass each event once: callers check replays for identity
//! instead of applying them again, which keeps a partial update that finds no
//! stored row a hard error.

use std::{collections::BTreeMap, fmt};

use crate::{
    data::{Cell, OldTableRow, PartialTableRow, TableRow, UpdatedTableRow},
    event::Event,
    schema::TableId,
};

/// Converts a [`TableRow`] into a typed record keyed by its row identity.
pub trait FromTableRow: Sized {
    /// Row identity used to address updates and deletes.
    type Id: Ord + Clone + fmt::Debug;

    /// Converts a full row, returning [`None`] when the row has another shape.
    fn from_table_row(table_row: &TableRow) -> Option<Self>;

    /// Returns the row identity.
    fn id(&self) -> Self::Id;

    /// Extracts the identity from a replica-identity key row.
    fn id_from_key_row(key_row: &TableRow) -> Option<Self::Id>;
}

/// Event payloads that the oracle cannot apply to the materialized table.
#[derive(Debug)]
pub enum MaterializeError {
    /// A full row did not convert to the record type.
    UnconvertibleRow {
        /// Debug rendering of the row values.
        values: String,
    },
    /// A key row did not yield an identity.
    UnconvertibleKeyRow {
        /// Debug rendering of the key values.
        values: String,
    },
    /// A delete carried no old-side row.
    DeleteWithoutOldRow,
    /// A partial update had no unique materialized row to complete it.
    PartialUpdateWithoutBase {
        /// Debug rendering of the present values.
        values: String,
    },
}

impl fmt::Display for MaterializeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnconvertibleRow { values } => write!(f, "Row {values} did not convert"),
            Self::UnconvertibleKeyRow { values } => write!(f, "Key row {values} did not convert"),
            Self::DeleteWithoutOldRow => write!(f, "Delete carried no old row"),
            Self::PartialUpdateWithoutBase { values } => {
                write!(f, "Partial update {values} had no unique base row")
            }
        }
    }
}

impl std::error::Error for MaterializeError {}

/// Returns the identity of a full row.
fn full_row_id<T: FromTableRow>(row: &TableRow) -> Result<T::Id, MaterializeError> {
    T::from_table_row(row)
        .map(|record| record.id())
        .ok_or_else(|| MaterializeError::UnconvertibleRow { values: format!("{:?}", row.values()) })
}

/// Returns the identity carried by an old-side row.
fn old_row_id<T: FromTableRow>(old_row: &OldTableRow) -> Result<T::Id, MaterializeError> {
    match old_row {
        OldTableRow::Full(row) => full_row_id::<T>(row),
        OldTableRow::Key(row) => T::id_from_key_row(row).ok_or_else(|| {
            MaterializeError::UnconvertibleKeyRow { values: format!("{:?}", row.values()) }
        }),
    }
}

/// Fills a partial row's missing columns from `base`.
fn merge_partial_row(base: &TableRow, partial: &PartialTableRow) -> TableRow {
    let mut present = partial.values().iter();
    let values = (0..partial.total_columns())
        .map(|index| {
            if partial.missing_column_indexes().contains(&index) {
                base.values().get(index).cloned().unwrap_or(Cell::Null)
            } else {
                present.next().cloned().unwrap_or(Cell::Null)
            }
        })
        .collect();

    TableRow::new(values)
}

/// Completes a partial update from the materialized row it modifies.
///
/// The old-side identity names the base row when present. Otherwise the base
/// is the unique row whose identity survives the merge, which holds whenever
/// the identity columns themselves are present in the partial row.
fn complete_partial_row<T: FromTableRow>(
    rows: &BTreeMap<T::Id, TableRow>,
    old_id: Option<&T::Id>,
    partial: &PartialTableRow,
) -> Result<TableRow, MaterializeError> {
    let missing_base =
        || MaterializeError::PartialUpdateWithoutBase { values: format!("{:?}", partial.values()) };

    if let Some(old_id) = old_id {
        let base = rows.get(old_id).ok_or_else(missing_base)?;
        return Ok(merge_partial_row(base, partial));
    }

    let mut candidates = rows.iter().filter_map(|(id, base)| {
        let merged = merge_partial_row(base, partial);
        let merged_id = T::from_table_row(&merged)?.id();
        (&merged_id == id).then_some(merged)
    });
    let merged = candidates.next().ok_or_else(missing_base)?;
    if candidates.next().is_some() {
        return Err(missing_base());
    }

    Ok(merged)
}

/// Materializes copied rows plus CDC events for one table.
///
/// `copied_rows` is the table-copy snapshot. Events then apply in order:
/// inserts upsert by identity, updates remove the old identity and upsert the
/// new row (completing partial rows from the stored row), deletes remove by
/// old-side identity, and truncates that include the table clear it. Events
/// for other tables are skipped.
pub fn materialize_events<T>(
    copied_rows: &[TableRow],
    events: &[Event],
    table_id: TableId,
) -> Result<BTreeMap<T::Id, T>, MaterializeError>
where
    T: FromTableRow,
{
    let mut rows = BTreeMap::<T::Id, TableRow>::new();

    for row in copied_rows {
        rows.insert(full_row_id::<T>(row)?, row.clone());
    }

    for event in events {
        match event {
            Event::Insert(insert) if insert.replicated_table_schema.id() == table_id => {
                rows.insert(full_row_id::<T>(&insert.table_row)?, insert.table_row.clone());
            }
            Event::Update(update) if update.replicated_table_schema.id() == table_id => {
                let old_id = update.old_table_row.as_ref().map(old_row_id::<T>).transpose()?;
                let new_row = match &update.updated_table_row {
                    UpdatedTableRow::Full(row) => row.clone(),
                    UpdatedTableRow::Partial(partial) => {
                        complete_partial_row::<T>(&rows, old_id.as_ref(), partial)?
                    }
                };
                if let Some(old_id) = old_id {
                    rows.remove(&old_id);
                }
                rows.insert(full_row_id::<T>(&new_row)?, new_row);
            }
            Event::Delete(delete) if delete.replicated_table_schema.id() == table_id => {
                let old_row =
                    delete.old_table_row.as_ref().ok_or(MaterializeError::DeleteWithoutOldRow)?;
                rows.remove(&old_row_id::<T>(old_row)?);
            }
            Event::Truncate(truncate)
                if truncate.truncated_tables.iter().any(|schema| schema.id() == table_id) =>
            {
                rows.clear();
            }
            _ => {}
        }
    }

    rows.into_iter()
        .map(|(id, row)| {
            T::from_table_row(&row).map(|record| (id, record)).ok_or_else(|| {
                MaterializeError::UnconvertibleRow { values: format!("{:?}", row.values()) }
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio_postgres::types::{PgLsn, Type};

    use super::*;
    use crate::{
        event::{DeleteEvent, InsertEvent, TruncateEvent, UpdateEvent},
        schema::{ColumnSchema, ReplicatedTableSchema, TableName, TableSchema},
    };

    /// Table every test event targets.
    const TABLE_ID: TableId = TableId(1);

    /// Row shape `(id, note, counter)` keyed by `id`.
    #[derive(Debug, PartialEq)]
    struct Record {
        /// Primary key.
        id: i64,
        /// Nullable note column.
        note: Option<String>,
        /// Counter column.
        counter: i32,
    }

    impl FromTableRow for Record {
        type Id = i64;

        fn from_table_row(table_row: &TableRow) -> Option<Self> {
            let [Cell::I64(id), note, Cell::I32(counter)] = table_row.values() else {
                return None;
            };
            let note = match note {
                Cell::Null => None,
                Cell::String(note) => Some(note.clone()),
                _ => return None,
            };

            Some(Self { id: *id, note, counter: *counter })
        }

        fn id(&self) -> Self::Id {
            self.id
        }

        fn id_from_key_row(key_row: &TableRow) -> Option<Self::Id> {
            let [Cell::I64(id)] = key_row.values() else {
                return None;
            };

            Some(*id)
        }
    }

    /// Returns the `(id, note, counter)` schema with a primary-key identity.
    fn schema() -> ReplicatedTableSchema {
        let table_schema = TableSchema::new(
            TABLE_ID,
            TableName::new("public".to_owned(), "t".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("note".to_owned(), Type::TEXT, -1, 2, true),
                ColumnSchema::new("counter".to_owned(), Type::INT4, -1, 3, false),
            ],
        );

        ReplicatedTableSchema::all(Arc::new(table_schema))
    }

    /// Builds a full row.
    fn row(id: i64, note: &str, counter: i32) -> TableRow {
        TableRow::new(vec![Cell::I64(id), Cell::String(note.to_owned()), Cell::I32(counter)])
    }

    /// Builds an expected materialized entry.
    fn record(id: i64, note: &str, counter: i32) -> (i64, Record) {
        (id, Record { id, note: Some(note.to_owned()), counter })
    }

    /// Builds an insert event.
    fn insert(table_row: TableRow) -> Event {
        Event::Insert(InsertEvent {
            commit_lsn: PgLsn::from(1),
            tx_ordinal: 0,
            replicated_table_schema: schema(),
            table_row,
        })
    }

    /// Builds an update event.
    fn update(updated_table_row: UpdatedTableRow, old_table_row: Option<OldTableRow>) -> Event {
        Event::Update(UpdateEvent {
            commit_lsn: PgLsn::from(1),
            tx_ordinal: 0,
            replicated_table_schema: schema(),
            updated_table_row,
            old_table_row,
        })
    }

    /// Builds a delete event carrying a key row.
    fn delete_key(id: i64) -> Event {
        Event::Delete(DeleteEvent {
            commit_lsn: PgLsn::from(1),
            tx_ordinal: 0,
            replicated_table_schema: schema(),
            old_table_row: Some(OldTableRow::Key(TableRow::new(vec![Cell::I64(id)]))),
        })
    }

    /// Builds a truncate event for the test table.
    fn truncate() -> Event {
        Event::Truncate(TruncateEvent {
            commit_lsn: PgLsn::from(1),
            tx_ordinal: 0,
            options: 0,
            truncated_tables: vec![schema()],
        })
    }

    #[test]
    fn truncate_clears_copied_and_streamed_rows() {
        let events = [insert(row(2, "b", 0)), truncate(), insert(row(3, "c", 0))];

        let materialized =
            materialize_events::<Record>(&[row(1, "a", 0)], &events, TABLE_ID).unwrap();

        assert_eq!(materialized, BTreeMap::from([record(3, "c", 0)]));
    }

    #[test]
    fn key_rows_address_deletes_and_key_changes() {
        let events = [
            insert(row(1, "a", 0)),
            insert(row(2, "b", 0)),
            update(
                UpdatedTableRow::Full(row(3, "a", 0)),
                Some(OldTableRow::Key(TableRow::new(vec![Cell::I64(1)]))),
            ),
            delete_key(2),
        ];

        let materialized = materialize_events::<Record>(&[], &events, TABLE_ID).unwrap();

        assert_eq!(materialized, BTreeMap::from([record(3, "a", 0)]));
    }

    #[test]
    fn partial_update_keeps_unchanged_columns_from_stored_row() {
        let partial =
            PartialTableRow::new(3, TableRow::new(vec![Cell::I64(1), Cell::I32(7)]), vec![1]);
        let events =
            [insert(row(1, "toasted", 0)), update(UpdatedTableRow::Partial(partial), None)];

        let materialized = materialize_events::<Record>(&[], &events, TABLE_ID).unwrap();

        assert_eq!(materialized, BTreeMap::from([record(1, "toasted", 7)]));
    }

    #[test]
    fn partial_update_without_stored_row_is_an_error() {
        let partial =
            PartialTableRow::new(3, TableRow::new(vec![Cell::I64(1), Cell::I32(7)]), vec![1]);
        let events = [update(UpdatedTableRow::Partial(partial), None)];

        let error = materialize_events::<Record>(&[], &events, TABLE_ID).unwrap_err();

        assert!(matches!(error, MaterializeError::PartialUpdateWithoutBase { .. }));
    }
}
