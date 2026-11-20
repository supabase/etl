//! Scenarios for the Snowflake destination's per-row request encoding.

use etl::{
    data::{Cell, TableRow},
    schema::{ColumnSchema, PgLsn, TableId, Type},
};
use etl_destinations::snowflake::{CdcMeta, CdcOperation, OffsetToken, RowBatchBuilder};

use crate::hot_paths::{
    Scenario, Workload,
    codec::{decoded_wide_rows, wide_column_schemas},
    fixtures::{Checksum, Rng},
};

/// Bytes in each generated `bytea` value.
const BYTEA_VALUE_BYTES: usize = 64;

/// Snowflake scenarios.
pub(super) const SCENARIOS: &[Scenario] =
    &[Scenario { name: "snowflake_row_encode", unit: "row", prepare: prepare_row_encode }];

/// Encodes CDC rows, each with its own offset, into Snowpipe request bodies.
struct RowEncodeWorkload {
    /// Destination columns in row order.
    column_schemas: Vec<ColumnSchema>,
    /// Decoded rows with a trailing `bytea` value.
    table_rows: Vec<TableRow>,
    /// Offset of each row, as the event path builds one per event.
    offsets: Vec<OffsetToken>,
    /// Hash of every request body of the latest pass.
    checksum: u64,
}

impl Workload for RowEncodeWorkload {
    fn units(&self) -> usize {
        self.table_rows.len()
    }

    fn run(&mut self) {
        let mut checksum = Checksum::new();
        let mut builder = RowBatchBuilder::new(TableId::new(1));
        for (table_row, offset) in self.table_rows.iter().zip(&self.offsets) {
            let cdc = CdcMeta::new(CdcOperation::Insert, offset.as_ref());
            let completed = builder
                .push_row(&self.column_schemas, table_row, cdc, offset)
                .expect("fixture rows encode");
            for batch in completed {
                checksum.update(batch.bytes());
            }
        }
        for batch in builder.finish().expect("fixture requests finish") {
            checksum.update(batch.bytes());
        }

        self.checksum = checksum.finish();
    }

    fn checksum(&mut self) -> u64 {
        self.checksum
    }
}

/// Prepares encoding of wide rows plus a `bytea` value at increasing offsets.
fn prepare_row_encode(mut rng: Rng, units: usize) -> Box<dyn Workload> {
    let mut column_schemas = wide_column_schemas();
    column_schemas.push(ColumnSchema::new("payload".to_owned(), Type::BYTEA, -1, 13, false));

    let table_rows = decoded_wide_rows(&mut rng, units)
        .into_iter()
        .map(|table_row| {
            let mut values = table_row.into_values();
            let bytes = (0..BYTEA_VALUE_BYTES.div_ceil(8))
                .flat_map(|_| rng.next_u64().to_le_bytes())
                .take(BYTEA_VALUE_BYTES)
                .collect();
            values.push(Cell::Bytes(bytes));
            TableRow::new(values)
        })
        .collect();
    let offsets = (0..units)
        .map(|ordinal| OffsetToken::new(PgLsn::from(1_000_u64), ordinal as u64))
        .collect();

    Box::new(RowEncodeWorkload { column_schemas, table_rows, offsets, checksum: 0 })
}
