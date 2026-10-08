//! CPU benchmark of the Snowflake request encoder.
//!
//! Feeds [`etl_destinations::snowflake::RowBatchBuilder`] prebuilt rows without
//! Postgres or Snowflake. Workloads are uninterrupted encoder streams, not a
//! simulation of source batch boundaries or destination latency.

use std::time::Instant;

use anyhow::{Context, Result, ensure};
use clap::Parser;
use etl::{
    data::{Cell, TableRow},
    schema::{ColumnSchema, TableId, Type},
};
use etl_destinations::snowflake::{
    CdcMeta, CdcOperation, Error as SnowflakeError, OffsetToken, RowBatch, RowBatchBuilder,
};
use serde::Serialize;

/// Snowflake request limit, mirrored from the encoder for fill percentages.
const REQUEST_LIMIT_BYTES: usize = 4 * 1024 * 1024;
/// Bytes in one mebibyte.
const MIB: usize = 1024 * 1024;
/// Payload budget for each prebuilt row pool, excluding row allocation
/// overhead.
///
/// A byte budget keeps even the 1 KiB pool well beyond the stream compressor's
/// history window. A row-count cap would introduce short, repeated sequences.
const MAX_PAYLOAD_VARIANT_BYTES: usize = 128 * MIB;

/// Fixed synthetic workloads for comparing encoder revisions.
const WORKLOADS: &[Workload] = &[
    Workload::structured("small_1kib", 200_000, 1024),
    Workload::structured("medium_64kib", 4_000, 64 * 1024),
    Workload::structured("below_cap_200kib", 1_000, 200 * 1024),
    Workload::structured("above_cap_300kib", 1_000, 300 * 1024),
    Workload {
        shape: PayloadShape::NearDuplicate,
        ..Workload::structured("near_duplicate_200kib", 1_000, 200 * 1024)
    },
    Workload {
        shape: PayloadShape::NearDuplicate,
        ..Workload::structured("near_duplicate_1mib", 256, MIB)
    },
    Workload::structured("large_1mib", 256, MIB),
    Workload::structured("large_2_5mib", 64, 5 * MIB / 2),
    Workload::structured("near_limit_13_6mb", 32, 13_600_000),
    Workload {
        shape: PayloadShape::RepeatedBlocks,
        ..Workload::structured("repeated_blocks_16mib", 16, 16 * MIB)
    },
    Workload {
        shape: PayloadShape::LateEntropy,
        ..Workload::structured("late_entropy_16mib", 16, 16 * MIB)
    },
    Workload {
        large_every: Some((100, MIB)),
        ..Workload::structured("mixed_small_with_1mib", 1_000, 1024)
    },
];

/// Command-line arguments for the encoder benchmark.
#[derive(Parser, Debug)]
#[command(author, version, about = "Snowflake request encoder benchmark", long_about = None)]
struct Args {
    /// Run only workloads whose name contains this text.
    #[arg(long)]
    filter: Option<String>,
    /// Measured runs per workload, after one discarded warm-up.
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u16).range(1..))]
    repeat: u16,
    /// Print one JSON object per workload instead of a table.
    #[arg(long, default_value_t = false)]
    json: bool,
}

/// Content distributions with distinct compression costs.
#[derive(Clone, Copy)]
enum PayloadShape {
    /// Independent structured records with moderately compressible padding.
    Structured,
    /// Adjacent values differ only in a short prefix.
    NearDuplicate,
    /// Repeated 3 MiB blocks exercise matches beyond the default stream window.
    RepeatedBlocks,
    /// A compressible prefix precedes a 4 MiB pseudo-random tail.
    LateEntropy,
}

/// One synthetic row stream; sizes describe TEXT payloads, not serialized rows.
struct Workload {
    /// Stable selector and report identifier.
    name: &'static str,
    /// Number of rows encoded per run.
    rows: usize,
    /// Ordinary TEXT payload length in bytes.
    row_bytes: usize,
    /// Every n-th row uses this larger payload length.
    large_every: Option<(usize, usize)>,
    /// Payload distribution shared by all ordinary variants.
    shape: PayloadShape,
}

impl Workload {
    /// Describes an independent structured-text stream.
    const fn structured(name: &'static str, rows: usize, row_bytes: usize) -> Self {
        Self { name, rows, row_bytes, large_every: None, shape: PayloadShape::Structured }
    }

    /// Total input payload bytes, excluding IDs and the serialized envelope.
    fn payload_bytes(&self) -> usize {
        match self.large_every {
            Some((every, bytes)) => {
                let large_rows = self.rows / every;
                large_rows * bytes + (self.rows - large_rows) * self.row_bytes
            }
            None => self.rows * self.row_bytes,
        }
    }
}

/// Generates deterministic ASCII text without external randomness or
/// credentials.
fn random_text(len: usize, seed: u64) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut state = seed;
    (0..len)
        .map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            ALPHABET[((state >> 33) % 62) as usize] as char
        })
        .collect()
}

/// Builds 256-byte records containing 88 pseudo-random letters plus padding.
fn structured_text(len: usize, seed: u64) -> String {
    let random = random_text((len / 256 + 1) * 88, seed);
    let mut payload = String::with_capacity(len + 256);
    for chunk in random.as_bytes().chunks_exact(88) {
        let start = payload.len();
        payload.push_str("{\"token\":\"");
        payload.push_str(std::str::from_utf8(chunk).unwrap());
        payload.push_str("\",\"padding\":\"");
        while payload.len() < start + 253 {
            payload.push('0');
        }
        payload.push_str("\"}\n");
        if payload.len() >= len {
            break;
        }
    }
    payload.truncate(len);
    payload
}

/// Prebuilt rows borrowed by the timed loop, as in the production copy path.
struct PreparedRows {
    /// Ordinary rows cycled by input index.
    ordinary: Vec<TableRow>,
    /// Occasional larger rows, present only in mixed workloads.
    large: Vec<TableRow>,
}

impl PreparedRows {
    /// Generates payloads and allocates cells before any timing starts.
    fn generate(workload: &Workload) -> Result<Self> {
        let ordinary = Self::variants(workload.row_bytes, workload.rows, 1, workload.shape)?;
        let large = match workload.large_every {
            Some((every, bytes)) => {
                Self::variants(bytes, workload.rows.div_ceil(every), 7, PayloadShape::Structured)?
            }
            None => Vec::new(),
        };
        Ok(Self { ordinary, large })
    }

    /// Builds a bounded pool; only the near-duplicate shape shares its content.
    fn variants(
        row_bytes: usize,
        rows: usize,
        seed: u64,
        shape: PayloadShape,
    ) -> Result<Vec<TableRow>> {
        let count = rows.min(MAX_PAYLOAD_VARIANT_BYTES / row_bytes).max(1);
        let shared = matches!(shape, PayloadShape::NearDuplicate)
            .then(|| structured_text(row_bytes, seed * 1_000_003));
        (0..count)
            .map(|i| {
                let variant_seed = seed * 1_000_003 + u64::try_from(i)?;
                let payload = match shape {
                    PayloadShape::Structured => structured_text(row_bytes, variant_seed),
                    PayloadShape::NearDuplicate => {
                        let mut payload = shared.as_ref().unwrap().clone();
                        payload.replace_range(..16, &format!("{i:016x}"));
                        payload
                    }
                    PayloadShape::RepeatedBlocks => {
                        let block = random_text(3 * MIB, variant_seed);
                        let mut payload = block.repeat(row_bytes.div_ceil(block.len()));
                        payload.truncate(row_bytes);
                        payload
                    }
                    PayloadShape::LateEntropy => {
                        debug_assert!(
                            row_bytes >= 4 * MIB,
                            "late-entropy fixtures include a 4 MiB tail"
                        );
                        let mut payload = "0".repeat(row_bytes - 4 * MIB);
                        payload.push_str(&random_text(4 * MIB, variant_seed));
                        payload
                    }
                };
                ensure!(payload.len() == row_bytes, "Fixture payload length changed");
                Ok(TableRow::new(vec![Cell::I32(i32::try_from(i)?), Cell::String(payload)]))
            })
            .collect()
    }

    /// Selects an existing row without cloning its payload.
    fn row(&self, workload: &Workload, index: usize) -> &TableRow {
        match workload.large_every {
            Some((every, _)) if index % every == every - 1 => {
                &self.large[(index / every) % self.large.len()]
            }
            _ => &self.ordinary[index % self.ordinary.len()],
        }
    }
}

/// One measured run; capacity values are observations between pushes only.
#[derive(Debug, Serialize)]
struct Sample {
    /// Total completed request-body bytes.
    compressed_bytes: usize,
    /// Number of completed requests, including the trailing one.
    requests: usize,
    /// Elapsed time for the selected measured run.
    elapsed_ms: f64,
    /// Input rows encoded per second.
    rows_per_second: f64,
    /// TEXT payload throughput, excluding IDs and wire overhead.
    payload_mib_per_second: f64,
    /// TEXT payload bytes divided by compressed request bytes.
    payload_compression_ratio: f64,
    /// Excludes the trailing request; absent when no earlier request exists.
    mean_nonfinal_fill_percent: Option<f64>,
    /// Excludes the trailing request; absent when no earlier request exists.
    min_nonfinal_fill_percent: Option<f64>,
    /// Rows compressed into independent frames.
    row_frames: usize,
    /// Largest scratch capacity observed after a push.
    sampled_max_scratch_capacity_bytes: usize,
    /// Largest open-body capacity observed after a push.
    sampled_max_body_capacity_bytes: usize,
}

/// A workload must accept every input row before its timing is meaningful.
#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Outcome {
    /// Middle measured run by elapsed time, plus timings in execution order.
    Accepted {
        /// Complete metrics from the middle measured run.
        #[serde(flatten)]
        sample: Sample,
        /// Individual measured times before sorting.
        elapsed_samples_ms: Vec<f64>,
    },
    /// A normally accepted fixture stopped at the encoder's row-size boundary.
    Rejected,
}

/// Machine-readable result for one workload, including failed experiments.
#[derive(Debug, Serialize)]
struct WorkloadReport {
    /// Stable workload identifier.
    workload: &'static str,
    /// Expected number of input rows.
    rows: usize,
    /// Expected TEXT payload bytes.
    payload_bytes: usize,
    /// Acceptance or a failed size experiment.
    #[serde(flatten)]
    outcome: Outcome,
}

/// Checks output accounting without retaining compressed bodies between rows.
fn record_batch(batch: RowBatch, sizes: &mut Vec<usize>, rows: &mut usize) -> Result<()> {
    ensure!(batch.size() > 0 && batch.size() <= REQUEST_LIMIT_BYTES, "Invalid request size");
    ensure!(batch.row_count() > 0, "Encoder emitted an empty request");
    *rows += batch.row_count();
    sizes.push(batch.size());
    Ok(())
}

/// Times borrowing, encoding, finalization and output accounting only.
fn run(workload: &Workload, rows: &PreparedRows) -> Result<Sample> {
    let cols = [
        ColumnSchema::new("id".into(), Type::INT4, -1, 1, true),
        ColumnSchema::new("payload".into(), Type::TEXT, -1, 2, true),
    ];
    let offset = OffsetToken::zero();
    let mut builder = RowBatchBuilder::new(TableId::new(1));
    let mut request_sizes = Vec::new();
    let mut emitted_rows = 0usize;
    let mut sampled_scratch = 0usize;
    let mut sampled_body = 0usize;

    let started = Instant::now();
    for index in 0..workload.rows {
        for batch in builder.push_row(
            &cols,
            rows.row(workload, index),
            CdcMeta::new(CdcOperation::Insert, "0"),
            &offset,
        )? {
            record_batch(batch, &mut request_sizes, &mut emitted_rows)?;
        }
        let footprint = builder.footprint();
        sampled_scratch = sampled_scratch.max(footprint.scratch_capacity);
        sampled_body = sampled_body.max(footprint.body_capacity);
    }
    let row_frames = builder.footprint().row_frames;
    for batch in builder.finish()? {
        record_batch(batch, &mut request_sizes, &mut emitted_rows)?;
    }
    let elapsed = started.elapsed();
    ensure!(emitted_rows == workload.rows, "Encoder output row count differs from input");

    let compressed_bytes: usize = request_sizes.iter().sum();
    ensure!(compressed_bytes > 0, "Encoder emitted no bytes");
    let nonfinal = &request_sizes[..request_sizes.len() - 1];
    let fill = |size: &usize| *size as f64 * 100.0 / REQUEST_LIMIT_BYTES as f64;
    let mean_fill = (!nonfinal.is_empty())
        .then(|| nonfinal.iter().map(fill).sum::<f64>() / nonfinal.len() as f64);
    let min_fill = nonfinal.iter().map(fill).reduce(f64::min);
    let seconds = elapsed.as_secs_f64();
    ensure!(seconds > 0.0, "Elapsed time is zero");

    Ok(Sample {
        compressed_bytes,
        requests: request_sizes.len(),
        elapsed_ms: seconds * 1000.0,
        rows_per_second: workload.rows as f64 / seconds,
        payload_mib_per_second: workload.payload_bytes() as f64 / MIB as f64 / seconds,
        payload_compression_ratio: workload.payload_bytes() as f64 / compressed_bytes as f64,
        mean_nonfinal_fill_percent: mean_fill,
        min_nonfinal_fill_percent: min_fill,
        row_frames,
        sampled_max_scratch_capacity_bytes: sampled_scratch,
        sampled_max_body_capacity_bytes: sampled_body,
    })
}

/// Discards a warm-up and retains the middle run plus every measured duration.
fn measure(workload: &Workload, rows: &PreparedRows, repeat: u16) -> Result<Outcome> {
    run(workload, rows)?;
    let mut samples = (0..repeat).map(|_| run(workload, rows)).collect::<Result<Vec<_>>>()?;
    let elapsed_samples_ms = samples.iter().map(|sample| sample.elapsed_ms).collect();
    samples.sort_by(|a, b| a.elapsed_ms.total_cmp(&b.elapsed_ms));
    let sample = samples.swap_remove(samples.len() / 2);
    Ok(Outcome::Accepted { sample, elapsed_samples_ms })
}

/// Preserves row-size failures as failed workloads, never as successful
/// timings.
fn benchmark(workload: &Workload, repeat: u16) -> Result<WorkloadReport> {
    let rows = PreparedRows::generate(workload)?;
    let outcome = match measure(workload, &rows, repeat) {
        Ok(outcome) => outcome,
        Err(error)
            if matches!(
                error.downcast_ref::<SnowflakeError>(),
                Some(SnowflakeError::RowTooLarge { .. })
            ) =>
        {
            Outcome::Rejected
        }
        Err(error) => {
            return Err(error).with_context(|| format!("Workload {} failed", workload.name));
        }
    };
    Ok(WorkloadReport {
        workload: workload.name,
        rows: workload.rows,
        payload_bytes: workload.payload_bytes(),
        outcome,
    })
}

/// Formats fill only when a nonfinal request exists.
fn format_fill(fill: Option<f64>) -> String {
    fill.map_or_else(|| "n/a".into(), |fill| format!("{fill:.1}%"))
}

/// Prints throughput alongside output size so speed cannot hide request growth.
fn print_table(reports: &[WorkloadReport]) {
    println!(
        "{:<26} {:>8} {:>10} {:>10} {:>10} {:>9} {:>10} {:>10} {:>7} {:>7}",
        "workload",
        "rows",
        "MiB/s",
        "rows/s",
        "output MiB",
        "requests",
        "mean fill*",
        "min fill*",
        "ratio",
        "frames"
    );
    for report in reports {
        match &report.outcome {
            Outcome::Accepted { sample, .. } => println!(
                "{:<26} {:>8} {:>10.1} {:>10.0} {:>10.2} {:>9} {:>10} {:>10} {:>7.2} {:>7}",
                report.workload,
                report.rows,
                sample.payload_mib_per_second,
                sample.rows_per_second,
                sample.compressed_bytes as f64 / MIB as f64,
                sample.requests,
                format_fill(sample.mean_nonfinal_fill_percent),
                format_fill(sample.min_nonfinal_fill_percent),
                sample.payload_compression_ratio,
                sample.row_frames,
            ),
            Outcome::Rejected => {
                println!("{:<26} REJECTED (expected every row to fit)", report.workload);
            }
        }
    }
    println!("* Fill excludes the trailing request. MiB/s and ratio use TEXT payload bytes.");
}

/// Runs selected workloads and fails if any normally accepted input was
/// rejected.
pub fn main() -> Result<()> {
    let args = Args::parse();
    let selected: Vec<_> = WORKLOADS
        .iter()
        .filter(|workload| args.filter.as_deref().is_none_or(|f| workload.name.contains(f)))
        .collect();
    ensure!(!selected.is_empty(), "No workloads match the filter");
    if cfg!(debug_assertions) {
        eprintln!("Warning: debug build; use --release for performance comparisons.");
    }

    let reports = selected
        .into_iter()
        .map(|workload| benchmark(workload, args.repeat))
        .collect::<Result<Vec<_>>>()?;
    if args.json {
        for report in &reports {
            println!("{}", serde_json::to_string(report)?);
        }
    } else {
        print_table(&reports);
    }
    ensure!(
        reports.iter().all(|report| matches!(report.outcome, Outcome::Accepted { .. })),
        "An encoder workload rejected a normally accepted row"
    );
    Ok(())
}
