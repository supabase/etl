//! CPU benchmark of the Snowflake request encoder.
//!
//! Feeds `RowBatchBuilder` directly with generated rows and reports
//! throughput, request count and fill, compression ratio, and peak buffer
//! capacities. Neither Postgres nor Snowflake is involved. Comparing with a
//! revision that lacks this benchmark requires a benchmark-only API adapter.

use std::time::Instant;

use anyhow::Result;
use clap::Parser;
use etl::{
    data::{Cell, TableRow},
    schema::{ColumnSchema, TableId, Type},
};
use etl_destinations::snowflake::{CdcMeta, CdcOperation, OffsetToken, RowBatchBuilder};
use serde::Serialize;

/// Snowflake request limit, mirrored from the encoder for fill percentages.
const REQUEST_LIMIT_BYTES: usize = 4 * 1024 * 1024;
const MIB: usize = 1024 * 1024;
/// Upper bound on pre-generated payload bytes per workload.
const MAX_PAYLOAD_VARIANT_BYTES: usize = 128 * MIB;

/// Command-line arguments for the encoder benchmark.
#[derive(Parser, Debug)]
#[command(author, version, about = "Snowflake request encoder benchmark", long_about = None)]
pub struct Args {
    /// Run only workloads whose name contains this text.
    #[arg(long)]
    filter: Option<String>,
    /// Repeat every workload this many times and report the fastest run.
    #[arg(long, default_value_t = 1)]
    repeat: usize,
    /// Print one JSON object per workload instead of a table.
    #[arg(long, default_value_t = false)]
    json: bool,
}

/// One synthetic row stream.
struct Workload {
    name: &'static str,
    /// Rows pushed through the builder.
    rows: usize,
    /// Payload length of an ordinary row.
    row_bytes: usize,
    /// Every n-th row carries this payload length instead, when set.
    large_every: Option<(usize, usize)>,
    /// Reuse the ordinary payload with a different short prefix in each
    /// variant.
    near_duplicate: bool,
}

const WORKLOADS: &[Workload] = &[
    Workload {
        name: "near_duplicate_1mib",
        rows: 256,
        row_bytes: MIB,
        large_every: None,
        near_duplicate: true,
    },
    Workload {
        name: "small_1kib",
        rows: 200_000,
        row_bytes: 1024,
        large_every: None,
        near_duplicate: false,
    },
    Workload {
        name: "medium_64kib",
        rows: 4_000,
        row_bytes: 64 * 1024,
        large_every: None,
        near_duplicate: false,
    },
    Workload {
        name: "above_cap_300kib",
        rows: 1_000,
        row_bytes: 300 * 1024,
        large_every: None,
        near_duplicate: false,
    },
    Workload {
        name: "large_1mib",
        rows: 256,
        row_bytes: MIB,
        large_every: None,
        near_duplicate: false,
    },
    Workload {
        name: "large_2_5mib",
        rows: 64,
        row_bytes: 5 * MIB / 2,
        large_every: None,
        near_duplicate: false,
    },
    Workload {
        name: "production_13_6mb",
        rows: 1,
        row_bytes: 13_600_000,
        large_every: None,
        near_duplicate: false,
    },
    Workload {
        name: "mixed_small_with_1mib",
        rows: 1_000,
        row_bytes: 1024,
        large_every: Some((100, MIB)),
        near_duplicate: false,
    },
];

/// Machine-readable result for one workload.
#[derive(Debug, Serialize)]
struct WorkloadReport {
    workload: &'static str,
    rows: usize,
    payload_bytes: usize,
    compressed_bytes: usize,
    requests: usize,
    elapsed_ms: f64,
    rows_per_second: f64,
    mib_per_second: f64,
    compression_ratio: f64,
    /// Mean fill of every request except the last, as a percentage of the
    /// limit.
    mean_fill_percent: f64,
    /// Smallest fill of every request except the last, as a percentage of the
    /// limit.
    min_fill_percent: f64,
    row_frames: usize,
    peak_scratch_bytes: usize,
    peak_body_bytes: usize,
}

/// Runs every selected workload and prints the results.
pub fn main() -> Result<()> {
    let args = Args::parse();
    let selected = WORKLOADS
        .iter()
        .filter(|workload| args.filter.as_deref().is_none_or(|f| workload.name.contains(f)));

    let mut reports = Vec::new();
    for workload in selected {
        let payloads = Payloads::generate(workload);
        let mut best: Option<WorkloadReport> = None;
        for _ in 0..args.repeat.max(1) {
            let report = run(workload, &payloads)?;
            if best.as_ref().is_none_or(|current| report.elapsed_ms < current.elapsed_ms) {
                best = Some(report);
            }
        }
        reports.extend(best);
    }

    if args.json {
        for report in &reports {
            println!("{}", serde_json::to_string(report)?);
        }
    } else {
        print_table(&reports);
    }
    Ok(())
}

/// Pre-generated payload variants, cycled through so compression sees varied
/// content without regenerating text inside the timed loop.
struct Payloads {
    ordinary: Vec<String>,
    large: Vec<String>,
}

impl Payloads {
    fn generate(workload: &Workload) -> Self {
        let ordinary =
            Self::variants(workload.row_bytes, workload.rows, 1, workload.near_duplicate);
        let large = match workload.large_every {
            Some((every, bytes)) => Self::variants(bytes, workload.rows / every + 1, 7, false),
            None => Vec::new(),
        };
        Self { ordinary, large }
    }

    fn variants(row_bytes: usize, rows: usize, seed: u64, near_duplicate: bool) -> Vec<String> {
        let count = rows.min(512).min(MAX_PAYLOAD_VARIANT_BYTES / row_bytes).max(1);
        let shared = near_duplicate.then(|| structured_text(row_bytes, seed * 1_000_003));
        (0..count)
            .map(|i| match &shared {
                Some(shared) => {
                    let mut payload = shared.clone();
                    // Preserve almost the entire previous row so this workload
                    // deliberately favors a shared compression history; it
                    // is a stress case, not typical traffic.
                    payload.replace_range(..16, &format!("{i:016x}"));
                    payload
                }
                None => structured_text(row_bytes, seed * 1_000_003 + i as u64),
            })
            .collect()
    }

    fn payload(&self, workload: &Workload, index: usize) -> &str {
        match workload.large_every {
            Some((every, _)) if index % every == every - 1 => {
                &self.large[(index / every) % self.large.len()]
            }
            _ => &self.ordinary[index % self.ordinary.len()],
        }
    }
}

fn run(workload: &Workload, payloads: &Payloads) -> Result<WorkloadReport> {
    let cols = [
        ColumnSchema::new("id".into(), Type::INT4, -1, 1, true),
        ColumnSchema::new("payload".into(), Type::TEXT, -1, 2, true),
    ];
    let offset = OffsetToken::zero();
    let mut builder = RowBatchBuilder::new(TableId::new(1));
    let mut request_sizes = Vec::new();
    let mut payload_bytes = 0usize;
    let mut peak_scratch = 0usize;
    let mut peak_body = 0usize;

    let started = Instant::now();
    for index in 0..workload.rows {
        let payload = payloads.payload(workload, index);
        payload_bytes += payload.len();
        let row = TableRow::new(vec![Cell::I32(index as i32), Cell::String(payload.to_owned())]);
        let completed =
            builder.push_row(&cols, &row, CdcMeta::new(CdcOperation::Insert, "0"), &offset)?;
        request_sizes.extend(completed.into_iter().map(|batch| batch.size()));
        let footprint = builder.footprint();
        peak_scratch = peak_scratch.max(footprint.scratch_capacity);
        peak_body = peak_body.max(footprint.body_capacity);
    }
    let row_frames = builder.footprint().row_frames;
    request_sizes.extend(builder.finish()?.into_iter().map(|batch| batch.size()));
    let elapsed = started.elapsed();

    let compressed_bytes: usize = request_sizes.iter().sum();
    let complete = &request_sizes[..request_sizes.len().saturating_sub(1)];
    let fill = |size: &usize| *size as f64 * 100.0 / REQUEST_LIMIT_BYTES as f64;
    let mean_fill_percent = if complete.is_empty() {
        0.0
    } else {
        complete.iter().map(fill).sum::<f64>() / complete.len() as f64
    };
    let min_fill_percent = complete.iter().map(fill).fold(f64::NAN, f64::min);
    let seconds = elapsed.as_secs_f64();

    Ok(WorkloadReport {
        workload: workload.name,
        rows: workload.rows,
        payload_bytes,
        compressed_bytes,
        requests: request_sizes.len(),
        elapsed_ms: seconds * 1000.0,
        rows_per_second: workload.rows as f64 / seconds,
        mib_per_second: payload_bytes as f64 / MIB as f64 / seconds,
        compression_ratio: payload_bytes as f64 / compressed_bytes.max(1) as f64,
        mean_fill_percent,
        min_fill_percent: if min_fill_percent.is_nan() { 0.0 } else { min_fill_percent },
        row_frames,
        peak_scratch_bytes: peak_scratch,
        peak_body_bytes: peak_body,
    })
}

fn print_table(reports: &[WorkloadReport]) {
    println!(
        "{:<24} {:>8} {:>10} {:>10} {:>9} {:>10} {:>9} {:>7} {:>7} {:>13} {:>13}",
        "workload",
        "rows",
        "MiB/s",
        "rows/s",
        "requests",
        "mean fill",
        "min fill",
        "ratio",
        "frames",
        "peak scratch",
        "peak body"
    );
    for report in reports {
        println!(
            "{:<24} {:>8} {:>10.1} {:>10.0} {:>9} {:>9.1}% {:>8.1}% {:>7.2} {:>7} {:>13} {:>13}",
            report.workload,
            report.rows,
            report.mib_per_second,
            report.rows_per_second,
            report.requests,
            report.mean_fill_percent,
            report.min_fill_percent,
            report.compression_ratio,
            report.row_frames,
            report.peak_scratch_bytes,
            report.peak_body_bytes
        );
    }
}

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

/// 256-byte records with 88 random letters each: structured text with a
/// compression ratio near 4, the same shape the encoder tests use.
fn structured_text(len: usize, seed: u64) -> String {
    let random = random_text((len / 256 + 1) * 88, seed);
    let mut payload = String::with_capacity(len + 256);
    for chunk in random.as_bytes().chunks_exact(88) {
        let start = payload.len();
        payload.push_str("{\"token\":\"");
        payload.push_str(std::str::from_utf8(chunk).unwrap_or_default());
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
