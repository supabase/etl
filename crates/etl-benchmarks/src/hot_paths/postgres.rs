//! Scenarios for Postgres value formatting and text parsing.
//!
//! Every destination formats numerics and temporal values through these
//! `Display` implementations, once per cell.

use std::{
    fmt::{self, Write},
    str::FromStr,
};

use etl::{
    benchmarking::parse_text_cell,
    data::{
        Cell, Date, PgNumeric, PgTime, PgTimeTz, Timestamp, format_date, format_timestamp,
        format_timestamptz,
    },
    schema::Type,
};

use crate::hot_paths::{
    Scenario, Workload,
    fixtures::{Rng, checksum_bytes, checksum_debug},
};

/// Postgres value scenarios.
pub(super) const SCENARIOS: &[Scenario] = &[
    Scenario { name: "numeric_display", unit: "cell", prepare: prepare_numeric_display },
    Scenario { name: "date_display", unit: "cell", prepare: prepare_date_display },
    Scenario { name: "time_display", unit: "cell", prepare: prepare_time_display },
    Scenario { name: "timetz_display", unit: "cell", prepare: prepare_timetz_display },
    Scenario { name: "timestamp_display", unit: "cell", prepare: prepare_timestamp_display },
    Scenario { name: "timestamptz_display", unit: "cell", prepare: prepare_timestamptz_display },
    Scenario { name: "timetz_parse", unit: "cell", prepare: prepare_timetz_parse },
];

/// Formats prebuilt values into one reused buffer, isolating the `Display`
/// cost from the caller's choice of output allocation.
struct DisplayWorkload<T, F> {
    /// Values to format.
    values: Vec<T>,
    /// Writes one value.
    write_value: F,
    /// Newline-separated output of the latest pass.
    output: String,
}

impl<T, F> DisplayWorkload<T, F>
where
    T: 'static,
    F: Fn(&mut String, &T) -> fmt::Result + 'static,
{
    /// Boxes a display workload over `values`.
    fn boxed(values: Vec<T>, write_value: F) -> Box<dyn Workload> {
        Box::new(Self { values, write_value, output: String::new() })
    }
}

impl<T, F> Workload for DisplayWorkload<T, F>
where
    F: Fn(&mut String, &T) -> fmt::Result,
{
    fn units(&self) -> usize {
        self.values.len()
    }

    fn run(&mut self) {
        self.output.clear();
        for value in &self.values {
            (self.write_value)(&mut self.output, value).expect("writing to a String cannot fail");
            self.output.push('\n');
        }
    }

    fn checksum(&mut self) -> u64 {
        checksum_bytes(self.output.as_bytes())
    }
}

/// Parses prebuilt Postgres text values of one type.
struct TextParseWorkload {
    /// Postgres type of every value.
    typ: Type,
    /// Text values as Postgres sends them.
    values: Vec<String>,
    /// Cells decoded by the latest pass.
    cells: Vec<Cell>,
}

impl Workload for TextParseWorkload {
    fn units(&self) -> usize {
        self.values.len()
    }

    fn run(&mut self) {
        self.cells.clear();
        for value in &self.values {
            self.cells
                .push(parse_text_cell(&self.typ, value).expect("fixture text parses as its type"));
        }
    }

    fn checksum(&mut self) -> u64 {
        checksum_debug(&self.cells)
    }
}

/// Prepares numeric formatting over production-shaped values.
fn prepare_numeric_display(mut rng: Rng, units: usize) -> Box<dyn Workload> {
    let values = (0..units)
        .map(|_| PgNumeric::from_str(&rng.numeric_text()).expect("fixture numeric text is valid"))
        .collect();

    DisplayWorkload::boxed(values, |output, value: &PgNumeric| write!(output, "{value}"))
}

/// Prepares date formatting.
fn prepare_date_display(mut rng: Rng, units: usize) -> Box<dyn Workload> {
    let values = (0..units).map(|_| Date::Value(rng.date())).collect();

    DisplayWorkload::boxed(values, |output, value| write!(output, "{}", format_date(value)))
}

/// Prepares time formatting.
fn prepare_time_display(mut rng: Rng, units: usize) -> Box<dyn Workload> {
    let values = (0..units).map(|_| PgTime::Value(rng.time())).collect();

    DisplayWorkload::boxed(values, |output, value: &PgTime| write!(output, "{value}"))
}

/// Prepares time with time zone formatting.
fn prepare_timetz_display(mut rng: Rng, units: usize) -> Box<dyn Workload> {
    let values = (0..units).map(|_| PgTimeTz::new(rng.time(), rng.utc_offset())).collect();

    DisplayWorkload::boxed(values, |output, value: &PgTimeTz| write!(output, "{value}"))
}

/// Prepares timestamp formatting.
fn prepare_timestamp_display(mut rng: Rng, units: usize) -> Box<dyn Workload> {
    let values = (0..units).map(|_| Timestamp::Value(rng.timestamp())).collect();

    DisplayWorkload::boxed(values, |output, value| write!(output, "{}", format_timestamp(value)))
}

/// Prepares timestamp with time zone formatting.
fn prepare_timestamptz_display(mut rng: Rng, units: usize) -> Box<dyn Workload> {
    let values = (0..units).map(|_| Timestamp::Value(rng.timestamptz())).collect();

    DisplayWorkload::boxed(values, |output, value| write!(output, "{}", format_timestamptz(value)))
}

/// Prepares time with time zone parsing from the text Postgres emits.
fn prepare_timetz_parse(mut rng: Rng, units: usize) -> Box<dyn Workload> {
    let values =
        (0..units).map(|_| PgTimeTz::new(rng.time(), rng.utc_offset()).to_string()).collect();

    Box::new(TextParseWorkload { typ: Type::TIMETZ, values, cells: Vec::new() })
}
