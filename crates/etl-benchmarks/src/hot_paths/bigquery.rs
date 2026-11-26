//! Scenarios for the BigQuery destination's per-row encoding.

use std::sync::Arc;

use etl::{
    data::TableRow,
    schema::{ReplicatedTableSchema, TableId, TableName, TableSchema},
};
use etl_destinations::bigquery::benchmarking::encode_table_row;

use crate::hot_paths::{
    Scenario, Workload,
    codec::{decoded_wide_rows, wide_column_schemas},
    fixtures::{Rng, checksum_bytes},
};

/// BigQuery scenarios.
pub(super) const SCENARIOS: &[Scenario] =
    &[Scenario { name: "bigquery_row_encode", unit: "row", prepare: prepare_row_encode }];

/// Encodes decoded rows into BigQuery's Protocol Buffer row bytes.
struct RowEncodeWorkload {
    /// Schema shared by the decoded rows.
    schema: ReplicatedTableSchema,
    /// Decoded rows to encode on every pass.
    table_rows: Vec<TableRow>,
    /// Copies of the rows that the next pass consumes.
    pending_table_rows: Vec<TableRow>,
    /// Concatenated row encodings of the latest pass.
    output: Vec<u8>,
}

impl Workload for RowEncodeWorkload {
    fn units(&self) -> usize {
        self.table_rows.len()
    }

    fn prepare_pass(&mut self) {
        self.pending_table_rows.clone_from(&self.table_rows);
    }

    fn run(&mut self) {
        self.output.clear();
        for table_row in self.pending_table_rows.drain(..) {
            encode_table_row(&self.schema, table_row, &mut self.output)
                .expect("fixture rows encode");
        }
    }

    fn checksum(&mut self) -> u64 {
        checksum_bytes(&self.output)
    }
}

/// Prepares encoding of rows with every commonly formatted cell type.
fn prepare_row_encode(mut rng: Rng, units: usize) -> Box<dyn Workload> {
    Box::new(RowEncodeWorkload {
        schema: ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "orders".to_owned()),
            wide_column_schemas(),
        ))),
        table_rows: decoded_wide_rows(&mut rng, units),
        pending_table_rows: Vec::new(),
        output: Vec::new(),
    })
}
