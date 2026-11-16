//! Scenarios for the ClickHouse destination's per-row encoding.

use etl::data::TableRow;
use etl_config::shared::ClickHouseEngine;
use etl_destinations::clickhouse::benchmarking::encode_copy_rows;

use crate::hot_paths::{
    Scenario, Workload,
    codec::decoded_wide_rows,
    fixtures::{Rng, checksum_bytes},
};

/// ClickHouse column types of the decoded wide rows, in column order, then the
/// MergeTree CDC columns.
///
/// Only the nullable and array wrappers affect RowBinary, so the inner types
/// just follow the destination's type mapping.
const WIDE_ROW_COLUMN_TYPES: &[&str] = &[
    "Int64",
    "Int32",
    "String",
    "String",
    "DateTime64(6)",
    "Date32",
    "Bool",
    "Nullable(String)",
    "DateTime64(6)",
    "String",
    "UUID",
    "String",
    "String",
    "UInt64",
    "UInt64",
];

/// ClickHouse scenarios.
pub(super) const SCENARIOS: &[Scenario] =
    &[Scenario { name: "clickhouse_row_encode", unit: "row", prepare: prepare_row_encode }];

/// Encodes decoded rows into one table-copy INSERT body.
struct RowEncodeWorkload {
    /// Decoded rows to encode on every pass.
    table_rows: Vec<TableRow>,
    /// Copies of the rows that the next pass consumes.
    pending_table_rows: Vec<TableRow>,
    /// RowBinary body of the latest pass.
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
        encode_copy_rows(
            self.pending_table_rows.drain(..),
            WIDE_ROW_COLUMN_TYPES,
            ClickHouseEngine::MergeTree,
            &mut self.output,
        )
        .expect("fixture rows encode");
    }

    fn checksum(&mut self) -> u64 {
        checksum_bytes(&self.output)
    }
}

/// Prepares encoding of rows with every commonly formatted cell type.
fn prepare_row_encode(mut rng: Rng, units: usize) -> Box<dyn Workload> {
    Box::new(RowEncodeWorkload {
        table_rows: decoded_wide_rows(&mut rng, units),
        pending_table_rows: Vec::new(),
        output: Vec::new(),
    })
}
