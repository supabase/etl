//! Scenarios for decoding source rows and recording their source metrics.
//!
//! The copy scenarios run once per COPY row during initial sync. The CDC
//! scenarios run once per pgoutput row-change message during streaming.

use std::sync::Arc;

use bytes::Bytes;
use etl::{
    benchmarking::{
        decode_row_change, parse_copy_rows, record_cdc_inserts_received, record_copy_rows_received,
    },
    data::TableRow,
    event::Event,
    schema::{ColumnSchema, ReplicatedTableSchema, TableId, TableName, TableSchema, Type},
};
use etl_telemetry::metrics::init_metrics_handle;

use crate::hot_paths::{
    Scenario, Workload,
    fixtures::{Checksum, Rng, checksum_debug},
};

/// Relation id written into generated pgoutput messages.
const RELATION_ID: u32 = 16_384;

/// Bytes in each generated `bytea` value.
const BYTEA_VALUE_BYTES: usize = 64;

/// Source decoding and metric scenarios.
pub(super) const SCENARIOS: &[Scenario] = &[
    Scenario { name: "copy_row_plain", unit: "row", prepare: prepare_copy_row_plain },
    Scenario { name: "copy_row_escaped", unit: "row", prepare: prepare_copy_row_escaped },
    Scenario { name: "copy_row_metrics", unit: "row", prepare: prepare_copy_row_metrics },
    Scenario { name: "cdc_insert_decode", unit: "row", prepare: prepare_cdc_insert_decode },
    Scenario { name: "cdc_update_decode", unit: "row", prepare: prepare_cdc_update_decode },
    Scenario { name: "cdc_insert_metrics", unit: "row", prepare: prepare_cdc_insert_metrics },
];

/// One Prometheus series whose growth a metrics scenario must reproduce on
/// every pass.
struct SeriesProbe {
    /// Rendered series name, including any `_count` or `_sum` suffix.
    name: &'static str,
    /// Rendered `key="value"` pairs the series must carry.
    labels: &'static [&'static str],
}

/// Series the copy path grows for each COPY row.
const COPY_ROW_PROBES: &[SeriesProbe] = &[
    SeriesProbe { name: "etl_row_size_bytes_count", labels: &["event_type=\"copy\""] },
    SeriesProbe { name: "etl_row_size_bytes_sum", labels: &["event_type=\"copy\""] },
];

/// Series the apply loop grows for each pgoutput insert message.
const CDC_INSERT_PROBES: &[SeriesProbe] = &[
    SeriesProbe { name: "etl_replication_messages_total", labels: &["worker_type=\"apply\""] },
    SeriesProbe {
        name: "etl_events_received_total",
        labels: &["worker_type=\"apply\"", "replication_path=\"cdc\""],
    },
    SeriesProbe { name: "etl_bytes_received_total", labels: &["event_type=\"insert\""] },
    SeriesProbe { name: "etl_row_size_bytes_count", labels: &["event_type=\"insert\""] },
    SeriesProbe { name: "etl_row_size_bytes_sum", labels: &["event_type=\"insert\""] },
];

/// Reads the current value of a probed series, or zero before it first
/// records.
fn series_value(rendered: &str, probe: &SeriesProbe) -> f64 {
    rendered
        .lines()
        .filter(|line| {
            line.strip_prefix(probe.name)
                .is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' '))
        })
        .find(|line| probe.labels.iter().all(|label| line.contains(label)))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0.0)
}

/// Tracks how far probed series advance between checksums.
struct SeriesGrowth {
    /// Series to read.
    probes: &'static [SeriesProbe],
    /// Values at the previous reading.
    previous: Vec<f64>,
}

impl SeriesGrowth {
    /// Starts tracking from the current series values.
    fn new(probes: &'static [SeriesProbe]) -> Self {
        let mut growth = Self { probes, previous: vec![0.0; probes.len()] };
        growth.checksum();

        growth
    }

    /// Hashes each series' growth since the previous call.
    ///
    /// Equal growth on every pass shows the recording reached the installed
    /// recorder under the expected names and labels.
    fn checksum(&mut self) -> u64 {
        let rendered = init_metrics_handle().expect("the metrics recorder is installed").render();
        let mut checksum = Checksum::new();
        for (probe, previous) in self.probes.iter().zip(&mut self.previous) {
            let current = series_value(&rendered, probe);
            checksum.update(&(current - *previous).to_le_bytes());
            *previous = current;
        }

        checksum.finish()
    }
}

/// Decodes prebuilt COPY text rows.
struct CopyRowWorkload {
    /// COPY row bodies, each ending in a newline.
    rows: Vec<Vec<u8>>,
    /// Schema of every row.
    column_schemas: Vec<ColumnSchema>,
    /// Rows decoded by the latest pass.
    table_rows: Vec<TableRow>,
}

impl Workload for CopyRowWorkload {
    fn units(&self) -> usize {
        self.rows.len()
    }

    fn run(&mut self) {
        self.table_rows.clear();
        parse_copy_rows(
            self.rows.iter().map(Vec::as_slice),
            &self.column_schemas,
            &mut self.table_rows,
        )
        .expect("fixture COPY rows decode");
    }

    fn checksum(&mut self) -> u64 {
        checksum_debug(&self.table_rows)
    }
}

/// Decodes prebuilt pgoutput row-change messages.
struct RowChangeWorkload {
    /// Message bodies as the replication stream delivers them.
    messages: Vec<Bytes>,
    /// Decoder schema for every message.
    replicated_table_schema: ReplicatedTableSchema,
    /// Events decoded by the latest pass.
    events: Vec<Event>,
}

impl Workload for RowChangeWorkload {
    fn units(&self) -> usize {
        self.messages.len()
    }

    fn run(&mut self) {
        self.events.clear();
        for message in &self.messages {
            self.events.push(
                decode_row_change(message, &self.replicated_table_schema)
                    .expect("fixture pgoutput messages decode"),
            );
        }
    }

    fn checksum(&mut self) -> u64 {
        checksum_debug(&self.events)
    }
}

/// Records source metrics for prebuilt payload sizes.
struct SourceMetricsWorkload {
    /// Payload bytes of each row.
    payload_bytes: Vec<u64>,
    /// Records the metrics for every row with one set of metric handles, as one
    /// copy stream or one apply loop does.
    record: fn(&[u64]),
    /// Growth of the series the recording must reach.
    growth: SeriesGrowth,
}

impl Workload for SourceMetricsWorkload {
    fn units(&self) -> usize {
        self.payload_bytes.len()
    }

    fn run(&mut self) {
        (self.record)(&self.payload_bytes);
    }

    fn checksum(&mut self) -> u64 {
        self.growth.checksum()
    }
}

/// Returns the schema of an order-like table with common scalar types.
fn plain_column_schemas() -> Vec<ColumnSchema> {
    vec![
        ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
        ColumnSchema::new("warehouse_id".to_owned(), Type::INT4, -1, 2, false),
        ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 3, false),
        ColumnSchema::new("balance".to_owned(), Type::NUMERIC, -1, 4, false),
        ColumnSchema::new("created_at".to_owned(), Type::TIMESTAMPTZ, -1, 5, false),
        ColumnSchema::new("delivery_date".to_owned(), Type::DATE, -1, 6, false),
        ColumnSchema::new("active".to_owned(), Type::BOOL, -1, 7, false),
        ColumnSchema::new("note".to_owned(), Type::TEXT, -1, 8, true),
    ]
}

/// Returns the Postgres text of one order-like row; `None` is SQL null.
fn plain_row_fields(rng: &mut Rng, id: usize) -> Vec<Option<String>> {
    let mut name = String::new();
    let name_length = 12 + rng.below(13) as usize;
    rng.push_letters(&mut name, name_length);

    let note = (!rng.one_in(4)).then(|| {
        let mut note = String::new();
        for word in 0..4 + rng.below(6) {
            if word > 0 {
                note.push(' ');
            }
            let word_length = 3 + rng.below(8) as usize;
            rng.push_letters(&mut note, word_length);
        }

        note
    });

    vec![
        Some(id.to_string()),
        Some(rng.below(1_000).to_string()),
        Some(name),
        Some(rng.numeric_text()),
        Some(format!("{}+00", rng.timestamp().format("%Y-%m-%d %H:%M:%S%.f"))),
        Some(rng.date().format("%Y-%m-%d").to_string()),
        Some(if rng.one_in(2) { "t" } else { "f" }.to_owned()),
        note,
    ]
}

/// Renders fields that need no escaping as one COPY text row.
fn copy_row(fields: &[Option<String>]) -> Vec<u8> {
    let mut row = Vec::new();
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            row.push(b'\t');
        }
        row.extend_from_slice(field.as_deref().unwrap_or("\\N").as_bytes());
    }
    row.push(b'\n');

    row
}

/// Returns decoded rows of an order-like table that also has the types
/// destinations format as text: timestamp, time, UUID, and JSON.
///
/// The rows come from the COPY decoder, so destination scenarios encode
/// exactly what a table copy would hand them.
#[cfg(feature = "bigquery")]
pub(super) fn decoded_wide_rows(rng: &mut Rng, units: usize) -> Vec<TableRow> {
    let mut column_schemas = plain_column_schemas();
    column_schemas.extend([
        ColumnSchema::new("updated_at".to_owned(), Type::TIMESTAMP, -1, 9, false),
        ColumnSchema::new("opens_at".to_owned(), Type::TIME, -1, 10, false),
        ColumnSchema::new("external_id".to_owned(), Type::UUID, -1, 11, false),
        ColumnSchema::new("attributes".to_owned(), Type::JSONB, -1, 12, false),
    ]);

    let rows: Vec<Vec<u8>> = (0..units)
        .map(|id| {
            let mut fields = plain_row_fields(rng, id);

            fields.push(Some(rng.timestamp().format("%Y-%m-%d %H:%M:%S%.f").to_string()));
            fields.push(Some(rng.time().format("%H:%M:%S%.f").to_string()));

            let mut uuid = String::new();
            for (index, bytes) in [4, 2, 2, 2, 6].into_iter().enumerate() {
                if index > 0 {
                    uuid.push('-');
                }
                rng.push_hex_bytes(&mut uuid, bytes);
            }
            fields.push(Some(uuid));

            let mut tier = String::new();
            rng.push_letters(&mut tier, 6);
            fields.push(Some(format!(
                "{{\"tier\": \"{tier}\", \"score\": {}, \"tags\": [\"a\", \"b\"]}}",
                rng.below(10_000)
            )));

            copy_row(&fields)
        })
        .collect();

    let mut table_rows = Vec::with_capacity(units);
    parse_copy_rows(rows.iter().map(Vec::as_slice), &column_schemas, &mut table_rows)
        .expect("fixture COPY rows decode");

    table_rows
}

/// Appends one pgoutput tuple with text-format fields.
fn push_tuple(message: &mut Vec<u8>, fields: &[Option<String>]) {
    let field_count = i16::try_from(fields.len()).expect("fixture rows have few columns");
    message.extend_from_slice(&field_count.to_be_bytes());
    for field in fields {
        match field {
            Some(value) => {
                let length = i32::try_from(value.len()).expect("fixture values are small");
                message.push(b't');
                message.extend_from_slice(&length.to_be_bytes());
                message.extend_from_slice(value.as_bytes());
            }
            None => message.push(b'n'),
        }
    }
}

/// Builds a pgoutput row-change message of `kind` carrying only a new tuple,
/// the shape Postgres sends for inserts and for updates that leave a default
/// replica identity unchanged.
fn new_tuple_message(kind: u8, fields: &[Option<String>]) -> Bytes {
    let mut message = vec![kind];
    message.extend_from_slice(&RELATION_ID.to_be_bytes());
    message.push(b'N');
    push_tuple(&mut message, fields);

    Bytes::from(message)
}

/// Prepares decoding of pgoutput messages of `kind` for the order-like table.
fn prepare_row_change_decode(mut rng: Rng, units: usize, kind: u8) -> Box<dyn Workload> {
    let messages =
        (0..units).map(|id| new_tuple_message(kind, &plain_row_fields(&mut rng, id))).collect();
    let table_schema = Arc::new(TableSchema::new(
        TableId::new(RELATION_ID),
        TableName::new("public".to_owned(), "orders".to_owned()),
        plain_column_schemas(),
    ));

    Box::new(RowChangeWorkload {
        messages,
        replicated_table_schema: ReplicatedTableSchema::all(table_schema),
        events: Vec::new(),
    })
}

/// Prepares COPY decoding of rows whose fields need no unescaping.
fn prepare_copy_row_plain(mut rng: Rng, units: usize) -> Box<dyn Workload> {
    let rows = (0..units).map(|id| copy_row(&plain_row_fields(&mut rng, id))).collect();

    Box::new(CopyRowWorkload {
        rows,
        column_schemas: plain_column_schemas(),
        table_rows: Vec::new(),
    })
}

/// Prepares COPY decoding of rows whose fields all carry backslash escapes.
///
/// COPY text doubles every backslash, so each `bytea` value (`\x…`), each JSON
/// document with a string escape, and each text value with a control character
/// goes through the unescape buffer.
fn prepare_copy_row_escaped(mut rng: Rng, units: usize) -> Box<dyn Workload> {
    let column_schemas = vec![
        ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
        ColumnSchema::new("payload".to_owned(), Type::BYTEA, -1, 2, false),
        ColumnSchema::new("document".to_owned(), Type::JSONB, -1, 3, false),
        ColumnSchema::new("body".to_owned(), Type::TEXT, -1, 4, false),
    ];
    let rows = (0..units)
        .map(|id| {
            let mut row = format!("{id}\t\\\\x");
            rng.push_hex_bytes(&mut row, BYTEA_VALUE_BYTES);

            row.push_str("\t{\"name\": \"");
            rng.push_letters(&mut row, 16);
            row.push_str("\", \"note\": \"first line\\\\nsecond line\", \"count\": ");
            row.push_str(&rng.below(100_000).to_string());
            row.push_str("}\t");

            rng.push_letters(&mut row, 24);
            row.push_str("\\t");
            rng.push_letters(&mut row, 24);
            row.push_str("\\n");
            rng.push_letters(&mut row, 24);
            row.push('\n');

            row.into_bytes()
        })
        .collect();

    Box::new(CopyRowWorkload { rows, column_schemas, table_rows: Vec::new() })
}

/// Prepares the row-size recording the copy path does for each COPY row.
fn prepare_copy_row_metrics(mut rng: Rng, units: usize) -> Box<dyn Workload> {
    let payload_bytes =
        (0..units).map(|id| copy_row(&plain_row_fields(&mut rng, id)).len() as u64).collect();

    Box::new(SourceMetricsWorkload {
        payload_bytes,
        record: record_copy_rows_received,
        growth: SeriesGrowth::new(COPY_ROW_PROBES),
    })
}

/// Prepares decoding of pgoutput insert messages.
fn prepare_cdc_insert_decode(rng: Rng, units: usize) -> Box<dyn Workload> {
    prepare_row_change_decode(rng, units, b'I')
}

/// Prepares decoding of pgoutput update messages that carry only a new tuple.
fn prepare_cdc_update_decode(rng: Rng, units: usize) -> Box<dyn Workload> {
    prepare_row_change_decode(rng, units, b'U')
}

/// Prepares the source metrics the apply loop records for each pgoutput insert
/// message.
fn prepare_cdc_insert_metrics(mut rng: Rng, units: usize) -> Box<dyn Workload> {
    let payload_bytes = (0..units)
        .map(|id| {
            plain_row_fields(&mut rng, id).iter().flatten().map(|field| field.len() as u64).sum()
        })
        .collect();

    Box::new(SourceMetricsWorkload {
        payload_bytes,
        record: record_cdc_inserts_received,
        growth: SeriesGrowth::new(CDC_INSERT_PROBES),
    })
}
