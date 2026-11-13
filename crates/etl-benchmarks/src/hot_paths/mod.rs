//! Instruction-count benchmark of per-row and per-cell hot paths.
//!
//! Each scenario prebuilds its fixtures without Postgres or a destination,
//! runs one discarded warm-up pass, then measures whole passes over the
//! fixtures. A pass keeps its output so a checksum can be taken outside the
//! measured window; the checksum proves the work ran and that two builds
//! produced the same result.
//!
//! Instructions retired are the primary metric because they vary far less
//! than wall time between two builds of nearly identical code. Allocation
//! counts are exact. Wall time is reported only as a cross-check.

#[cfg(feature = "bigquery")]
mod bigquery;
mod codec;
mod counters;
mod fixtures;
mod postgres;

use anyhow::{Result, ensure};
use clap::Parser;
pub use counters::CountingAllocator;
use serde::Serialize;

use crate::hot_paths::{
    counters::{Counters, Usage},
    fixtures::Rng,
};

/// Default fixture seed; any fixed value works, comparisons only need the
/// same one on both sides.
const DEFAULT_SEED: u64 = 0x00e7_1b5e_edc0_ffee;

/// Scenario groups in report order.
const SCENARIO_GROUPS: &[&[Scenario]] = &[
    postgres::SCENARIOS,
    codec::SCENARIOS,
    #[cfg(feature = "bigquery")]
    bigquery::SCENARIOS,
];

/// Command-line arguments for the hot-path benchmark.
#[derive(Parser, Debug)]
#[command(author, version, about = "Instruction-count benchmark of ETL hot paths", long_about = None)]
struct Args {
    /// Run only scenarios whose name contains this text.
    #[arg(long)]
    filter: Option<String>,
    /// Rows or cells each pass processes.
    #[arg(long, default_value_t = 20_000, value_parser = clap::value_parser!(u32).range(1..))]
    units: u32,
    /// Measured passes per scenario, after one discarded warm-up.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u16).range(1..))]
    repeat: u16,
    /// Seed for fixture generation.
    #[arg(long, default_value_t = DEFAULT_SEED)]
    seed: u64,
    /// Print one JSON object per scenario instead of a table.
    #[arg(long, default_value_t = false)]
    json: bool,
}

/// A prepared scenario whose fixtures are built and whose passes can be
/// measured.
trait Workload {
    /// Returns how many units one pass processes.
    fn units(&self) -> usize;

    /// Rebuilds input that a pass consumes, outside the measured window.
    fn prepare_pass(&mut self) {}

    /// Runs one pass over every unit and keeps its output.
    fn run(&mut self);

    /// Hashes the output of the latest pass, outside the measured window.
    fn checksum(&mut self) -> u64;
}

/// One named hot path and how to prepare its fixtures.
struct Scenario {
    /// Stable name used by `--filter` and in reports.
    name: &'static str,
    /// What one unit is, such as `row` or `cell`.
    unit: &'static str,
    /// Builds fixtures for the requested unit count from a scenario-specific
    /// random stream.
    prepare: fn(Rng, usize) -> Box<dyn Workload>,
}

/// Median per-unit costs of one scenario.
#[derive(Debug, Serialize)]
struct ScenarioReport {
    /// Scenario name.
    scenario: &'static str,
    /// What one unit is.
    unit: &'static str,
    /// Units processed by each pass.
    units: usize,
    /// Instructions retired per unit in the median pass, when the platform
    /// reports them.
    instructions_per_unit: Option<f64>,
    /// Range of instruction counts across passes, as a percentage of the
    /// median.
    instructions_spread_percent: Option<f64>,
    /// Allocator calls per unit in the median pass.
    allocations_per_unit: f64,
    /// Bytes requested from the allocator per unit in the median pass.
    allocated_bytes_per_unit: f64,
    /// Wall-clock nanoseconds per unit in the median pass.
    nanoseconds_per_unit: f64,
    /// Hash of the output, identical for every pass.
    checksum: String,
}

/// Runs the warm-up and measured passes for one scenario.
fn measure(scenario: &Scenario, args: &Args) -> Result<ScenarioReport> {
    let rng = Rng::for_scenario(args.seed, scenario.name);
    let mut workload = (scenario.prepare)(rng, args.units as usize);
    let units = workload.units();
    ensure!(units > 0, "Scenario {} prepared no units", scenario.name);

    // The warm-up faults in output buffers and fills allocator caches, so the
    // measured passes see steady-state costs.
    workload.prepare_pass();
    workload.run();
    let checksum = workload.checksum();

    let mut samples: Vec<Usage> = Vec::with_capacity(usize::from(args.repeat));
    for _ in 0..args.repeat {
        workload.prepare_pass();
        let before = Counters::read();
        workload.run();
        let usage = Counters::read().since(before);

        ensure!(
            workload.checksum() == checksum,
            "Scenario {} produced different output between passes",
            scenario.name
        );
        samples.push(usage);
    }

    samples.sort_by_key(|usage| (usage.instructions, usage.elapsed));
    let median = samples[samples.len() / 2];
    let per_unit = |value: u64| value as f64 / units as f64;
    let instruction_range = samples
        .first()
        .and_then(|usage| usage.instructions)
        .zip(samples.last().and_then(|usage| usage.instructions));

    Ok(ScenarioReport {
        scenario: scenario.name,
        unit: scenario.unit,
        units,
        instructions_per_unit: median.instructions.map(per_unit),
        instructions_spread_percent: median.instructions.zip(instruction_range).map(
            |(median, (lowest, highest))| (highest - lowest) as f64 * 100.0 / median.max(1) as f64,
        ),
        allocations_per_unit: per_unit(median.allocations),
        allocated_bytes_per_unit: per_unit(median.allocated_bytes),
        nanoseconds_per_unit: median.elapsed.as_nanos() as f64 / units as f64,
        checksum: format!("{checksum:016x}"),
    })
}

/// Formats an optional measurement, marking counters the platform lacks.
fn format_optional(value: Option<f64>, precision: usize, suffix: &str) -> String {
    value.map_or_else(|| "n/a".into(), |value| format!("{value:.precision$}{suffix}"))
}

/// Prints per-unit costs with the spread that bounds a trustworthy
/// difference.
fn print_table(reports: &[ScenarioReport]) {
    println!(
        "{:<24} {:>5} {:>11} {:>8} {:>11} {:>10} {:>8}  {:<16}",
        "scenario",
        "unit",
        "instr/unit",
        "spread",
        "allocs/unit",
        "bytes/unit",
        "ns/unit",
        "checksum"
    );
    for report in reports {
        println!(
            "{:<24} {:>5} {:>11} {:>8} {:>11.3} {:>10.1} {:>8.1}  {:<16}",
            report.scenario,
            report.unit,
            format_optional(report.instructions_per_unit, 1, ""),
            format_optional(report.instructions_spread_percent, 2, "%"),
            report.allocations_per_unit,
            report.allocated_bytes_per_unit,
            report.nanoseconds_per_unit,
            report.checksum,
        );
    }
}

/// Runs the selected scenarios and prints their reports.
pub fn main() -> Result<()> {
    let args = Args::parse();
    let selected: Vec<_> = SCENARIO_GROUPS
        .iter()
        .flat_map(|group| group.iter())
        .filter(|scenario| args.filter.as_deref().is_none_or(|f| scenario.name.contains(f)))
        .collect();
    ensure!(!selected.is_empty(), "No scenarios match the filter");
    if cfg!(debug_assertions) {
        eprintln!("Warning: debug build; use --release for performance comparisons.");
    }

    // Production always runs with the Prometheus recorder installed, so metric
    // calls inside every scenario pay the real registry cost. Installing it
    // spawns an upkeep task, which needs a runtime context; the runtime is
    // never driven, so no background work runs during measured passes.
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let _runtime_context = runtime.enter();
    etl_telemetry::metrics::init_metrics_handle()?;

    let reports = selected
        .into_iter()
        .map(|scenario| measure(scenario, &args))
        .collect::<Result<Vec<_>>>()?;
    if args.json {
        for report in &reports {
            println!("{}", serde_json::to_string(report)?);
        }
    } else {
        print_table(&reports);
    }

    Ok(())
}
