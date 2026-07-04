use std::{
    collections::HashSet,
    fmt::Write as _,
    time::{SystemTime, UNIX_EPOCH},
};

use etl::{
    bail,
    data::{ArrayCell, Cell, OldTableRow, TableRow, UpdatedTableRow},
    error::{ErrorKind, EtlResult},
    etl_error,
    event::EventSequenceKey,
    schema::{ColumnModification, ColumnSchema, ReplicatedTableSchema, SchemaDiff},
};
use sha2::{Digest, Sha256};

use super::core::{
    bigquery_delete_old_row, bigquery_update_new_row, ensure_bigquery_key_image_matches_primary_key,
};
use crate::bigquery::encoding::BigQueryTableRow;

const BIGQUERY_SEQUENCE_ORDINAL_FIRST: u64 = 0;
const BIGQUERY_SEQUENCE_ORDINAL_SECOND: u64 = 1;
const APPEND_ONLY_COPY_SORT_TIMESTAMP: i64 = i64::MIN;
const POSTGRES_EPOCH_UNIX_SECONDS: i64 = 946_684_800;
const MICROS_PER_SECOND: i64 = 1_000_000;

/// Append-only change operation.
#[derive(Debug, Clone, Copy)]
pub(super) enum AppendOnlyChangeType {
    Insert,
    UpdateDelete,
    UpdateInsert,
    Delete,
}

impl AppendOnlyChangeType {
    /// Returns the stored change type string.
    fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::UpdateDelete => "UPDATE-DELETE",
            Self::UpdateInsert => "UPDATE-INSERT",
            Self::Delete => "DELETE",
        }
    }
}

/// Metadata stored alongside every append-only row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AppendOnlyMetadata {
    pub(super) uuid: String,
    pub(super) source_timestamp: i64,
    pub(super) change_sequence_number: String,
    pub(super) sort_keys: Vec<String>,
}

/// Append-only row paired with its durable replay sequence number.
pub(super) struct AppendOnlyTableRow {
    row: BigQueryTableRow,
    sequence_number: String,
}

impl AppendOnlyTableRow {
    pub(super) fn new(row: BigQueryTableRow, sequence_number: String) -> Self {
        Self { row, sequence_number }
    }

    pub(super) fn row(&self) -> &BigQueryTableRow {
        &self.row
    }

    pub(super) fn sequence_number(&self) -> &str {
        &self.sequence_number
    }

    pub(super) fn into_parts(self) -> (BigQueryTableRow, String) {
        (self.row, self.sequence_number)
    }
}

/// Builds an append-only sequence number for copied rows.
pub(super) fn append_only_copy_sequence_number(row_index: u64) -> String {
    format!("0000000000000000/0000000000000000/{row_index:016x}")
}

fn append_only_copy_stable_sequence_number(row_hash: &str) -> String {
    format!("0000000000000000/0000000000000000/{row_hash}")
}

/// Returns a process-local timestamp for initial-copy metadata in Postgres
/// timestamp wire format.
pub(super) fn append_only_backfill_timestamp() -> EtlResult<i64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|err| {
        etl_error!(
            ErrorKind::InvalidState,
            "System clock is before the Unix epoch",
            err.to_string()
        )
    })?;

    let unix_micros = duration
        .as_secs()
        .checked_mul(MICROS_PER_SECOND as u64)
        .and_then(|micros| micros.checked_add(u64::from(duration.subsec_micros())))
        .ok_or_else(|| {
            etl_error!(
                ErrorKind::InvalidState,
                "Current timestamp exceeded supported range",
                "Initial-copy metadata timestamp overflowed"
            )
        })?;
    let postgres_epoch_micros = POSTGRES_EPOCH_UNIX_SECONDS * MICROS_PER_SECOND;

    i64::try_from(unix_micros)
        .ok()
        .and_then(|micros| micros.checked_sub(postgres_epoch_micros))
        .ok_or_else(|| {
            etl_error!(
                ErrorKind::InvalidState,
                "Current timestamp exceeded supported range",
                "Initial-copy metadata timestamp overflowed"
            )
        })
}

/// Builds an append-only sequence number for streamed rows.
pub(super) fn append_only_sequence_number(
    sequence_key: EventSequenceKey,
    internal_ordinal: u64,
) -> String {
    format!("{sequence_key}/{internal_ordinal:016x}")
}

/// Builds metadata for an initial-copy row.
pub(super) fn append_only_copy_metadata(
    replicated_table_schema: &ReplicatedTableSchema,
    table_row: &TableRow,
    source_timestamp: i64,
    row_index: u64,
) -> AppendOnlyMetadata {
    let row_hash = append_only_copy_row_hash(replicated_table_schema, table_row);
    let sequence_number = if append_only_copy_uses_primary_key_identity(replicated_table_schema) {
        append_only_copy_stable_sequence_number(&row_hash)
    } else {
        append_only_copy_sequence_number(row_index)
    };
    let uuid = format!("copy/{sequence_number}/{row_hash}");
    AppendOnlyMetadata {
        uuid: uuid.clone(),
        source_timestamp,
        change_sequence_number: sequence_number.clone(),
        sort_keys: vec![
            append_only_timestamp_sort_key(APPEND_ONLY_COPY_SORT_TIMESTAMP),
            sequence_number,
            uuid,
        ],
    }
}

/// Builds metadata for a CDC-derived append-only row.
pub(super) fn append_only_event_metadata(
    sequence_key: EventSequenceKey,
    internal_ordinal: u64,
    source_timestamp: i64,
) -> AppendOnlyMetadata {
    let sequence_number = append_only_sequence_number(sequence_key, internal_ordinal);
    AppendOnlyMetadata {
        uuid: sequence_number.clone(),
        source_timestamp,
        change_sequence_number: sequence_number.clone(),
        sort_keys: vec![
            append_only_timestamp_sort_key(source_timestamp),
            sequence_key.to_string(),
            format!("{internal_ordinal:016x}"),
        ],
    }
}

/// Converts an `i64` into a lexicographically sortable hexadecimal key.
pub(super) fn append_only_timestamp_sort_key(timestamp: i64) -> String {
    format!("{:016x}", (timestamp as u64) ^ (1_u64 << 63))
}

/// Builds a deterministic row identity for initial-copy rows.
fn append_only_copy_row_hash(
    replicated_table_schema: &ReplicatedTableSchema,
    table_row: &TableRow,
) -> String {
    let mut identity = String::new();
    let _ = write!(
        &mut identity,
        "table_id={};table_name={};snapshot={};",
        replicated_table_schema.id(),
        replicated_table_schema.name(),
        replicated_table_schema.inner().snapshot_id
    );

    let use_primary_key_identity =
        append_only_copy_uses_primary_key_identity(replicated_table_schema);
    let _ = write!(&mut identity, "identity=primary_key:{use_primary_key_identity};");

    for (column_schema, cell) in replicated_table_schema.column_schemas().zip(table_row.values()) {
        if use_primary_key_identity && !column_schema.primary_key() {
            continue;
        }

        let _ = write!(
            &mut identity,
            "column={}#{}:{};",
            column_schema.name,
            column_schema.ordinal_position,
            stable_cell_identity(cell)
        );
    }

    let digest = Sha256::digest(identity.as_bytes());
    let mut hash = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(&mut hash, "{byte:02x}");
    }
    hash
}

fn append_only_copy_uses_primary_key_identity(
    replicated_table_schema: &ReplicatedTableSchema,
) -> bool {
    replicated_table_schema.all_primary_key_columns_replicated()
        && replicated_table_schema.primary_key_column_schemas().len() > 0
}

/// Produces a length-prefixed representation to avoid simple delimiter
/// collisions in initial-copy row hashing.
fn stable_cell_identity(cell: &Cell) -> String {
    let value = format!("{cell:?}");
    format!("{}:{value}", value.len())
}

/// Returns physical columns that must be added for append-only schema changes.
pub(super) fn append_only_schema_columns_to_add(diff: &SchemaDiff) -> Vec<&ColumnSchema> {
    let mut seen_column_names = HashSet::new();
    let mut columns = Vec::new();

    for column in &diff.columns_to_add {
        if seen_column_names.insert(column.name.clone()) {
            columns.push(column);
        }
    }

    for change in &diff.columns_to_change {
        if change
            .modifications
            .iter()
            .any(|modification| matches!(modification, ColumnModification::Rename { .. }))
            && seen_column_names.insert(change.new_column.name.clone())
        {
            columns.push(&change.new_column);
        }
    }

    columns
}

/// Builds one or two append-only rows for an update event.
pub(super) fn append_only_update_rows(
    replicated_table_schema: &ReplicatedTableSchema,
    old_table_row: Option<OldTableRow>,
    updated_table_row: UpdatedTableRow,
    sequence_key: EventSequenceKey,
    source_timestamp: i64,
) -> EtlResult<Vec<AppendOnlyTableRow>> {
    let new_table_row = bigquery_update_new_row(replicated_table_schema, updated_table_row)?;
    // Postgres sends a key-only old image when the replica identity key
    // changed, so expand it to record the old key's removal in the history.
    let old_table_row = match old_table_row {
        Some(OldTableRow::Full(row)) => Some(row),
        Some(OldTableRow::Key(row)) => {
            Some(key_row_to_sparse_table_row(replicated_table_schema, row)?)
        }
        None => None,
    };
    let mut rows = Vec::with_capacity(2);

    if let Some(old_table_row) = old_table_row {
        rows.push(append_only_tracked_row(
            replicated_table_schema,
            old_table_row,
            AppendOnlyChangeType::UpdateDelete,
            append_only_event_metadata(
                sequence_key,
                BIGQUERY_SEQUENCE_ORDINAL_FIRST,
                source_timestamp,
            ),
        )?);
        rows.push(append_only_tracked_row(
            replicated_table_schema,
            new_table_row,
            AppendOnlyChangeType::UpdateInsert,
            append_only_event_metadata(
                sequence_key,
                BIGQUERY_SEQUENCE_ORDINAL_SECOND,
                source_timestamp,
            ),
        )?);
    } else {
        rows.push(append_only_tracked_row(
            replicated_table_schema,
            new_table_row,
            AppendOnlyChangeType::UpdateInsert,
            append_only_event_metadata(
                sequence_key,
                BIGQUERY_SEQUENCE_ORDINAL_FIRST,
                source_timestamp,
            ),
        )?);
    }

    Ok(rows)
}

/// Returns the old row required for append-only delete events.
pub(super) fn append_only_delete_old_row(
    replicated_table_schema: &ReplicatedTableSchema,
    old_table_row: Option<OldTableRow>,
) -> EtlResult<TableRow> {
    let old_table_row = bigquery_delete_old_row(replicated_table_schema, old_table_row)?;
    match old_table_row {
        OldTableRow::Full(row) => Ok(row),
        OldTableRow::Key(row) => key_row_to_sparse_table_row(replicated_table_schema, row),
    }
}

/// Expands a dense primary-key row into a sparse table row.
fn key_row_to_sparse_table_row(
    replicated_table_schema: &ReplicatedTableSchema,
    key_row: TableRow,
) -> EtlResult<TableRow> {
    ensure_bigquery_key_image_matches_primary_key(replicated_table_schema)?;

    let mut key_values = key_row.into_values().into_iter();
    let mut values = Vec::with_capacity(replicated_table_schema.column_schemas().len());
    for column_schema in replicated_table_schema.column_schemas() {
        if column_schema.primary_key() {
            let Some(value) = key_values.next() else {
                bail!(
                    ErrorKind::InvalidState,
                    "Append-only key image shape is inconsistent",
                    format!(
                        "Table '{}' key image ended before all primary key values",
                        replicated_table_schema.name()
                    )
                );
            };
            values.push(value);
        } else {
            values.push(Cell::Null);
        }
    }

    if key_values.next().is_some() {
        bail!(
            ErrorKind::InvalidState,
            "Append-only key image has leftover values",
            format!(
                "Table '{}' key image contained more values than its primary key",
                replicated_table_schema.name()
            )
        );
    }

    Ok(TableRow::new(values))
}

/// Builds an append-only JSON row with ETL metadata.
pub(super) fn append_only_row(
    replicated_table_schema: &ReplicatedTableSchema,
    table_row: TableRow,
    change_type: AppendOnlyChangeType,
    metadata: AppendOnlyMetadata,
) -> EtlResult<BigQueryTableRow> {
    let column_count = replicated_table_schema.column_schemas().len();
    if table_row.values().len() != column_count {
        bail!(
            ErrorKind::InvalidState,
            "Append-only row image does not match the replicated schema",
            format!(
                "Expected {} values for table '{}', got {}",
                column_count,
                replicated_table_schema.name(),
                table_row.values().len()
            )
        );
    }

    let mut tagged_cells = table_row
        .into_values()
        .into_iter()
        .enumerate()
        .map(|(index, cell)| (index + 1, cell))
        .collect::<Vec<_>>();

    tagged_cells.extend(append_only_metadata_tagged_cells(column_count, change_type, metadata));

    BigQueryTableRow::try_from_tagged_cells(tagged_cells)
}

/// Builds an append-only row and keeps its sequence number next to it.
pub(super) fn append_only_tracked_row(
    replicated_table_schema: &ReplicatedTableSchema,
    table_row: TableRow,
    change_type: AppendOnlyChangeType,
    metadata: AppendOnlyMetadata,
) -> EtlResult<AppendOnlyTableRow> {
    let sequence_number = metadata.change_sequence_number.clone();
    let row = append_only_row(replicated_table_schema, table_row, change_type, metadata)?;
    Ok(AppendOnlyTableRow::new(row, sequence_number))
}

/// Builds flat append-only metadata cells using descriptor field numbers after
/// the source columns.
fn append_only_metadata_tagged_cells(
    source_column_count: usize,
    change_type: AppendOnlyChangeType,
    metadata: AppendOnlyMetadata,
) -> Vec<(usize, Cell)> {
    vec![
        (source_column_count + 1, Cell::String(metadata.uuid)),
        (source_column_count + 2, Cell::I64(metadata.source_timestamp)),
        (source_column_count + 3, Cell::String(metadata.change_sequence_number)),
        (source_column_count + 4, Cell::String(change_type.as_str().to_owned())),
        (
            source_column_count + 5,
            Cell::Array(ArrayCell::String(metadata.sort_keys.into_iter().map(Some).collect())),
        ),
    ]
}
